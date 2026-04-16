#!/usr/bin/env python3

# This file is part of BenchExec, a framework for reliable benchmarking:
# https://github.com/sosy-lab/benchexec
#
# SPDX-FileCopyrightText: 2007-2020 Dirk Beyer <https://www.sosy-lab.org>
# SPDX-FileCopyrightText: 2024-2026 Levente Bajczi
# SPDX-FileCopyrightText: Critical Systems Research Group
# SPDX-FileCopyrightText: Budapest University of Technology and Economics <https://www.ftsrg.mit.bme.hu>
#
# SPDX-License-Identifier: Apache-2.0

"""Worker script executed inside each SLURM array task.

Reads its task from a manifest file, executes the tool command,
monitors cgroup resource usage, and writes results to a results directory.

Usage: python3 slurm_worker.py <manifest.json> <task_index> <results_dir>
"""

import json
import os
import platform
import shlex
import subprocess
import sys
import threading
import time


def find_own_cgroup():
    """Find the cgroup v2 path for the current process."""
    try:
        with open("/proc/self/cgroup", "r") as f:
            for line in f:
                line = line.strip()
                # cgroup v2 format: "0::<path>"
                if line.startswith("0::"):
                    rel_path = line[3:]
                    # Find cgroup2 mount point
                    mount_point = _find_cgroup2_mount()
                    if mount_point:
                        return os.path.join(mount_point, rel_path.lstrip("/"))
    except OSError:
        pass
    return None


def _find_cgroup2_mount():
    """Find the cgroup2 mount point from /proc/mounts."""
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[2] == "cgroup2":
                    return parts[1]
    except OSError:
        pass
    return "/sys/fs/cgroup"


def read_cgroup_cpu_time(cgroup_path):
    """Read CPU time in seconds from cgroup v2 cpu.stat (usage_usec)."""
    stat_file = os.path.join(cgroup_path, "cpu.stat")
    try:
        with open(stat_file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2 and parts[0] == "usage_usec":
                    return int(parts[1]) / 1_000_000
    except OSError:
        pass
    return None


def read_cgroup_memory_peak(cgroup_path):
    """Read peak memory usage in bytes from cgroup v2 memory.peak."""
    peak_file = os.path.join(cgroup_path, "memory.peak")
    try:
        with open(peak_file, "r") as f:
            val = f.read().strip()
            if val != "max":
                return int(val)
    except OSError:
        pass
    # Fallback: read memory.current (not peak, but better than nothing)
    current_file = os.path.join(cgroup_path, "memory.current")
    try:
        with open(current_file, "r") as f:
            return int(f.read().strip())
    except OSError:
        pass
    return None


def read_cgroup_memory_current(cgroup_path):
    """Read current memory usage for polling-based peak tracking."""
    current_file = os.path.join(cgroup_path, "memory.current")
    try:
        with open(current_file, "r") as f:
            return int(f.read().strip())
    except OSError:
        pass
    return None


def read_cgroup_oom_kill_count(cgroup_path):
    """Check if an OOM kill occurred in this cgroup."""
    events_file = os.path.join(cgroup_path, "memory.events")
    try:
        with open(events_file, "r") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) == 2 and parts[0] == "oom_kill":
                    return int(parts[1])
    except OSError:
        pass
    return 0


def collect_system_info():
    """Collect basic system info from the worker node."""
    info = {
        "hostname": platform.node(),
        "os": platform.platform(aliased=True),
    }

    # CPU model from /proc/cpuinfo
    try:
        with open("/proc/cpuinfo", "r") as f:
            cores = 0
            for line in f:
                if line.startswith("model name"):
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        info["cpu_model"] = (
                            parts[1]
                            .strip()
                            .replace("(R)", "")
                            .replace("(TM)", "")
                            .replace("(tm)", "")
                        )
                if line.startswith("processor"):
                    cores += 1
            info["cpu_number_of_cores"] = str(cores)
    except OSError:
        pass

    # CPU max frequency
    try:
        with open(
            "/sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq", "r"
        ) as f:
            info["cpu_max_frequency"] = int(f.read().strip()) * 1000  # kHz -> Hz
    except OSError:
        pass

    # Memory from /proc/meminfo
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    parts = line.split()
                    if len(parts) >= 2:
                        info["memory"] = int(parts[1]) * 1024  # kB -> bytes
                    break
    except OSError:
        pass

    return info


class MemoryPeakTracker:
    """Polls cgroup memory.current to track peak memory as a fallback
    when memory.peak is not available (Linux < 5.19)."""

    def __init__(self, cgroup_path, interval=0.1):
        self.cgroup_path = cgroup_path
        self.interval = interval
        self.peak = 0
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def _poll(self):
        while not self._stop.is_set():
            mem = read_cgroup_memory_current(self.cgroup_path)
            if mem is not None and mem > self.peak:
                self.peak = mem
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        return self.peak


def run_task(task, results_dir, task_index):
    """Execute a single benchmarking task and record results."""
    args = task["cmdline"]
    log_file_basename = task["log_file_basename"]

    result_file = os.path.join(results_dir, f"{task_index}.json")
    log_file = os.path.join(results_dir, f"{task_index}.log")

    cgroup_path = find_own_cgroup()
    has_memory_peak = cgroup_path and os.path.exists(
        os.path.join(cgroup_path, "memory.peak")
    )

    # Start memory peak tracker if memory.peak is unavailable
    mem_tracker = None
    if cgroup_path and not has_memory_peak:
        mem_tracker = MemoryPeakTracker(cgroup_path)
        mem_tracker.start()

    # Read initial cgroup CPU time
    cpu_before = None
    if cgroup_path:
        cpu_before = read_cgroup_cpu_time(cgroup_path)

    # Record wall time
    wall_start = time.monotonic()

    # Execute the tool
    try:
        with open(log_file, "w") as log_f:
            # Write the 6-line header that BenchExec expects
            log_f.write(shlex.join(args) + "\n")
            log_f.write("\n")
            log_f.write("\n")
            log_f.write("-" * 80 + "\n")
            log_f.write("\n")
            log_f.write("\n")
            log_f.flush()

            proc = subprocess.run(
                args,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                timeout=task.get("walltime_limit"),
            )
            returncode = proc.returncode
            timed_out = False
    except subprocess.TimeoutExpired:
        returncode = 9  # SIGKILL-like
        timed_out = True
        # Append timeout message to log
        with open(log_file, "a") as log_f:
            log_f.write("\n\nWall time limit exceeded.\n")
    except OSError as e:
        returncode = 1
        timed_out = False
        with open(log_file, "a") as log_f:
            log_f.write(f"\n\nFailed to execute: {e}\n")

    wall_end = time.monotonic()
    walltime = wall_end - wall_start

    # Read final cgroup values
    cputime = None
    if cgroup_path and cpu_before is not None:
        cpu_after = read_cgroup_cpu_time(cgroup_path)
        if cpu_after is not None:
            cputime = cpu_after - cpu_before

    memory = None
    if cgroup_path:
        if has_memory_peak:
            memory = read_cgroup_memory_peak(cgroup_path)
        elif mem_tracker:
            memory = mem_tracker.stop()

    oom_count = 0
    if cgroup_path:
        oom_count = read_cgroup_oom_kill_count(cgroup_path)

    # Determine termination reason
    termination_reason = None
    if timed_out:
        termination_reason = "walltime"
    elif oom_count > 0:
        termination_reason = "memory"
    elif task.get("cputime_limit") and cputime and cputime > task["cputime_limit"]:
        termination_reason = "cputime"

    # Collect system info
    sysinfo = collect_system_info()

    result = {
        "returncode": returncode,
        "walltime": walltime,
        "cputime": cputime,
        "memory": memory,
        "terminationreason": termination_reason,
        "host": sysinfo.get("hostname"),
        "systeminfo": sysinfo,
    }

    with open(result_file, "w") as f:
        json.dump(result, f, indent=2)

    return 0


def main():
    if len(sys.argv) != 4:
        print(
            f"Usage: {sys.argv[0]} <manifest.json> <task_index> <results_dir>",
            file=sys.stderr,
        )
        sys.exit(1)

    manifest_path = sys.argv[1]
    task_index = int(sys.argv[2])
    results_dir = sys.argv[3]

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    if task_index < 0 or task_index >= len(manifest["tasks"]):
        print(f"Task index {task_index} out of range", file=sys.stderr)
        sys.exit(1)

    task = manifest["tasks"][task_index]
    sys.exit(run_task(task, results_dir, task_index))


if __name__ == "__main__":
    main()
