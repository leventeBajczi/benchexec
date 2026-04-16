# This file is part of BenchExec, a framework for reliable benchmarking:
# https://github.com/sosy-lab/benchexec
#
# SPDX-FileCopyrightText: 2007-2020 Dirk Beyer <https://www.sosy-lab.org>
# SPDX-FileCopyrightText: 2024-2026 Levente Bajczi
# SPDX-FileCopyrightText: Critical Systems Research Group
# SPDX-FileCopyrightText: Budapest University of Technology and Economics <https://www.ftsrg.mit.bme.hu>
#
# SPDX-License-Identifier: Apache-2.0

"""Two-stage SLURM executor for BenchExec.

Stage 1 (submit): Generates a SLURM array job. Each array task executes one
    benchexec run, monitors its own cgroup-based resource usage, and writes
    results (JSON + log) to a shared directory.

Stage 2 (collect): Reads the results directory and produces normal BenchExec
    output (XML, tables, etc.).

Resource measurement relies on cgroup v2 accounting provided by SLURM itself
(no custom cgroup creation needed).
"""

import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time

from benchexec import BenchExecException, tooladapter
from benchexec.util import ProcessExitCode

sys.dont_write_bytecode = True

STOPPED_BY_INTERRUPT = False

# Path to the worker script, relative to this file
_WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "slurm_worker.py")


def init(config, benchmark):
    tool_locator = tooladapter.create_tool_locator(config)
    benchmark.executable = benchmark.tool.executable(tool_locator)
    benchmark.tool_version = benchmark.tool.version(benchmark.executable)


def get_system_info():
    # System info is collected per-node by the worker; we don't have a single
    # system to report at the top level.
    return None


def execute_benchmark(benchmark, output_handler):
    if not benchmark.config.scratchdir:
        sys.exit(
            "No scratchdir specified. Use --scratchdir <path> to set a shared "
            "directory accessible from all SLURM nodes."
        )

    mode = getattr(benchmark.config, "slurm_mode", "both")

    for runSet in benchmark.run_sets:
        if STOPPED_BY_INTERRUPT:
            break

        if not runSet.should_be_executed():
            output_handler.output_for_skipping_run_set(runSet)
        elif not runSet.runs:
            output_handler.output_for_skipping_run_set(
                runSet, "because it has no files"
            )
        else:
            _execute_run_set(runSet, benchmark, output_handler, mode)

    output_handler.output_after_benchmark(STOPPED_BY_INTERRUPT)
    return 0


def stop():
    global STOPPED_BY_INTERRUPT
    STOPPED_BY_INTERRUPT = True


def _execute_run_set(runSet, benchmark, output_handler, mode):
    walltime_before = time.monotonic()
    output_handler.output_before_run_set(runSet)

    # Create a results directory for this run set
    results_dir = _results_dir_for_run_set(benchmark, runSet)
    os.makedirs(results_dir, exist_ok=True)

    if mode in ("submit", "both"):
        _stage_submit(runSet, benchmark, results_dir)

    if mode == "submit":
        logging.info(
            "Stage 1 complete: SLURM jobs submitted. Results will be written to %s",
            results_dir,
        )
        logging.info(
            "Run again with --slurm-mode=collect to gather results after jobs finish."
        )
        # We still need to call output_after_run_set for well-formedness,
        # but skip per-run output
        walltime_after = time.monotonic()
        output_handler.output_after_run_set(
            runSet, walltime=walltime_after - walltime_before
        )
        return

    if mode in ("collect", "both"):
        _stage_collect(runSet, benchmark, output_handler, results_dir)

    walltime_after = time.monotonic()

    if STOPPED_BY_INTERRUPT:
        output_handler.set_error("interrupted", runSet)

    output_handler.output_after_run_set(
        runSet, walltime=walltime_after - walltime_before
    )


def _results_dir_for_run_set(benchmark, runSet):
    """Deterministic results directory path for a run set."""
    scratchdir = benchmark.config.scratchdir
    bench_name = benchmark.name
    runset_name = runSet.real_name or f"runset_{runSet.index}"
    return os.path.join(scratchdir, f"slurm_results_{bench_name}", runset_name)


# ---------------------------------------------------------------------------
# Stage 1: Submit
# ---------------------------------------------------------------------------


def _stage_submit(runSet, benchmark, results_dir):
    """Create a manifest and submit a SLURM array job."""
    manifest = _create_manifest(runSet, benchmark)
    manifest_path = os.path.join(results_dir, "manifest.json")

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    num_tasks = len(manifest["tasks"])
    if num_tasks == 0:
        return

    logging.info(
        "Submitting SLURM array job with %d tasks for run set %s",
        num_tasks,
        runSet.real_name or runSet.index,
    )

    job_script = _generate_job_script(benchmark, manifest_path, results_dir)
    job_script_path = os.path.join(results_dir, "job.sh")
    with open(job_script_path, "w") as f:
        f.write(job_script)
    os.chmod(job_script_path, 0o755)

    # Submit via sbatch
    sbatch_cmd = ["sbatch", "--parsable"]

    # Resource limits
    timelimit = benchmark.rlimits.cputime
    walltime = benchmark.rlimits.walltime
    # Use walltime if available, else use cputime with a generous margin
    effective_limit = walltime or (timelimit * 2 if timelimit else None)
    if effective_limit:
        h = int(effective_limit // 3600)
        m = int((effective_limit % 3600) // 60)
        s = int(effective_limit % 60)
        sbatch_cmd.extend(["-t", f"{h}:{m:02d}:{s:02d}"])

    cpus = benchmark.rlimits.cpu_cores
    if cpus:
        sbatch_cmd.extend(["-c", str(cpus)])

    memory = benchmark.rlimits.memory
    if memory:
        sbatch_cmd.extend(["--mem", f"{int(memory / 1_000_000)}M"])

    sbatch_cmd.extend(["--threads-per-core=1"])
    sbatch_cmd.extend([f"--array=0-{num_tasks - 1}"])

    # Limit concurrent tasks if num_of_threads is set
    if benchmark.num_of_threads and benchmark.num_of_threads < num_tasks:
        sbatch_cmd[-1] += f"%{benchmark.num_of_threads}"

    sbatch_cmd.extend(["--job-name", f"benchexec_{benchmark.name}"])
    sbatch_cmd.extend(
        [
            "--output",
            os.path.join(results_dir, "slurm_%A_%a.out"),
        ]
    )

    # Extra SLURM options from config
    extra = getattr(benchmark.config, "slurm_sbatch_args", None)
    if extra:
        sbatch_cmd.extend(shlex.split(extra))

    sbatch_cmd.append(job_script_path)

    logging.debug("sbatch command: %s", shlex.join(sbatch_cmd))

    try:
        result = subprocess.run(
            sbatch_cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        job_id = result.stdout.strip().split(";")[0]
        logging.info("Submitted SLURM array job %s", job_id)

        # Save job ID for tracking
        with open(os.path.join(results_dir, "job_id"), "w") as f:
            f.write(job_id)

    except subprocess.CalledProcessError as e:
        raise BenchExecException(
            f"Failed to submit SLURM job: {e.stderr.strip()}"
        ) from e

    # In "both" mode, wait for the job to complete
    _wait_for_job(job_id, results_dir, num_tasks)


def _create_manifest(runSet, benchmark):
    """Create a JSON manifest describing all tasks in a run set."""
    tasks = []
    for i, run in enumerate(runSet.runs):
        task = {
            "index": i,
            "cmdline": run.cmdline(),
            "log_file_basename": os.path.basename(run.log_file),
            "identifier": run.identifier,
        }
        if benchmark.rlimits.cputime:
            task["cputime_limit"] = benchmark.rlimits.cputime
        if benchmark.rlimits.walltime:
            task["walltime_limit"] = benchmark.rlimits.walltime
        if benchmark.rlimits.memory:
            task["memory_limit"] = benchmark.rlimits.memory
        tasks.append(task)

    return {"tasks": tasks}


def _generate_job_script(benchmark, manifest_path, results_dir):
    """Generate a SLURM batch script that runs the worker."""
    worker_script = os.path.abspath(_WORKER_SCRIPT)
    manifest_abs = os.path.abspath(manifest_path)
    results_abs = os.path.abspath(results_dir)

    singularity = getattr(benchmark.config, "singularity", None)

    if singularity:
        # Inside singularity: bind CWD as /lower, results dir, and the worker
        worker_cmd = (
            f"singularity exec"
            f" -B ./:/lower"
            f" --no-home"
            f" -B {shlex.quote(results_abs)}:{shlex.quote(results_abs)}"
            f" -B {shlex.quote(os.path.dirname(worker_script))}:"
            f"{shlex.quote(os.path.dirname(worker_script))}"
            f" {shlex.quote(singularity)}"
            f" python3 {shlex.quote(worker_script)}"
            f" {shlex.quote(manifest_abs)}"
            f" $SLURM_ARRAY_TASK_ID"
            f" {shlex.quote(results_abs)}"
        )
    else:
        worker_cmd = (
            f"python3 {shlex.quote(worker_script)}"
            f" {shlex.quote(manifest_abs)}"
            f" $SLURM_ARRAY_TASK_ID"
            f" {shlex.quote(results_abs)}"
        )

    return f"""#!/bin/bash
#SBATCH --ntasks=1
# Auto-generated by BenchExec SLURM executor

{worker_cmd}
"""


def _wait_for_job(job_id, results_dir, num_tasks):
    """Wait for a SLURM array job to complete."""
    logging.info(
        "Waiting for SLURM job %s to complete (%d tasks)...", job_id, num_tasks
    )

    while not STOPPED_BY_INTERRUPT:
        try:
            result = subprocess.run(
                [
                    "squeue",
                    "--job",
                    str(job_id),
                    "--noheader",
                    "--format=%t",
                ],
                capture_output=True,
                text=True,
            )
            # If no lines in output, all tasks are done
            active = [
                line.strip()
                for line in result.stdout.strip().split("\n")
                if line.strip()
            ]
            if not active:
                logging.info("All SLURM tasks completed for job %s", job_id)
                break

            running = sum(1 for s in active if s == "R")
            pending = sum(1 for s in active if s == "PD")
            logging.debug(
                "Job %s: %d running, %d pending, %d other",
                job_id,
                running,
                pending,
                len(active) - running - pending,
            )
        except OSError as e:
            logging.warning("Failed to check job status: %s", e)

        time.sleep(10)

    if STOPPED_BY_INTERRUPT:
        logging.info("Cancelling SLURM job %s", job_id)
        try:
            subprocess.run(["scancel", str(job_id)], check=False)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Stage 2: Collect
# ---------------------------------------------------------------------------


def _stage_collect(runSet, benchmark, output_handler, results_dir):
    """Read results from the results directory and produce BenchExec output."""
    manifest_path = os.path.join(results_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise BenchExecException(
            f"Manifest not found at {manifest_path}. "
            f"Run with --slurm-mode=submit first."
        )

    for i, run in enumerate(runSet.runs):
        if STOPPED_BY_INTERRUPT:
            break

        output_handler.output_before_run(run)

        result_file = os.path.join(results_dir, f"{i}.json")
        worker_log = os.path.join(results_dir, f"{i}.log")

        if not os.path.exists(result_file):
            logging.warning(
                "No result for task %d (%s) - SLURM task may have failed",
                i,
                run.identifier,
            )
            _write_empty_log(run, "No result from SLURM worker.")
            values = {
                "exitcode": ProcessExitCode.create(value=1),
                "terminationreason": "failed",
            }
        else:
            with open(result_file, "r") as f:
                task_result = json.load(f)

            # Copy the worker's log to the expected BenchExec log path
            if os.path.exists(worker_log):
                os.makedirs(os.path.dirname(run.log_file), exist_ok=True)
                shutil.copy2(worker_log, run.log_file)
            else:
                _write_empty_log(run, "Worker log file not found.")

            values = _parse_worker_result(task_result)

        run.set_result(values)
        output_handler.output_after_run(run)


def _parse_worker_result(task_result):
    """Convert worker JSON result to the dict expected by run.set_result()."""
    values = {}

    returncode = task_result.get("returncode", 1)
    values["exitcode"] = ProcessExitCode.create(value=max(0, min(255, returncode)))

    if task_result.get("walltime") is not None:
        values["walltime"] = task_result["walltime"]

    if task_result.get("cputime") is not None:
        values["cputime"] = task_result["cputime"]

    if task_result.get("memory") is not None:
        values["memory"] = task_result["memory"]

    if task_result.get("terminationreason"):
        values["terminationreason"] = task_result["terminationreason"]

    if task_result.get("host"):
        values["host"] = task_result["host"]

    return values


def _write_empty_log(run, message):
    """Write a log file with the expected 6-line header and a message."""
    os.makedirs(os.path.dirname(run.log_file), exist_ok=True)
    with open(run.log_file, "w") as f:
        f.write("(command not available)\n")
        f.write("\n")
        f.write("\n")
        f.write("-" * 80 + "\n")
        f.write("\n")
        f.write("\n")
        f.write(message + "\n")
