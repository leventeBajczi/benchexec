# This file is part of BenchExec, a framework for reliable benchmarking:
# https://github.com/sosy-lab/benchexec
#
# SPDX-FileCopyrightText: 2007-2020 Dirk Beyer <https://www.sosy-lab.org>
# SPDX-FileCopyrightText: 2024-2026 Levente Bajczi
# SPDX-FileCopyrightText: Critical Systems Research Group
# SPDX-FileCopyrightText: Budapest University of Technology and Economics <https://www.ftsrg.mit.bme.hu>
#
# SPDX-License-Identifier: Apache-2.0

"""Two-stage SLURM executor for BenchExec (offline workflow).

Stage 1 (generate): Creates a self-contained directory with a SLURM array job
    script, a manifest of all runs, and the worker script. This directory is
    meant to be transferred to an HPC login node and submitted manually.

Stage 2 (collect): After the SLURM jobs finish and the results directory is
    copied back, this stage reads the per-task results and produces standard
    BenchExec output (XML, tables, log files).

Typical workflow:
    1. Local:  python3 slurm-benchmark.py --slurm --slurm-mode=generate ...
    2. User:   scp -r <scratchdir>/slurm_<benchmark>/ hpc:~/jobs/
    3. HPC:    cd ~/jobs/slurm_<benchmark>/<runset>/ && sbatch job.sh
    4. User:   scp -r hpc:~/jobs/slurm_<benchmark>/ <scratchdir>/
    5. Local:  python3 slurm-benchmark.py --slurm --slurm-mode=collect ...

Resource measurement relies on cgroup v2 accounting provided by SLURM itself
(no custom cgroup creation needed).
"""

import json
import logging
import os
import shlex
import shutil
import sys
import time

from benchexec import BenchExecException, tooladapter
from benchexec.util import ProcessExitCode

sys.dont_write_bytecode = True

STOPPED_BY_INTERRUPT = False

# Path to the worker script, relative to this file
_WORKER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "slurm_worker.py")


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
            "No scratchdir specified. Use --scratchdir <path> to set the "
            "directory where the SLURM job bundle will be generated."
        )

    mode = getattr(benchmark.config, "slurm_mode", "generate")

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

    results_dir = _results_dir_for_run_set(benchmark, runSet)
    os.makedirs(results_dir, exist_ok=True)

    if mode == "generate":
        _stage_generate(runSet, benchmark, results_dir)
        walltime_after = time.monotonic()
        output_handler.output_after_run_set(
            runSet, walltime=walltime_after - walltime_before
        )
        return

    if mode == "collect":
        _stage_collect(runSet, benchmark, output_handler, results_dir)
        walltime_after = time.monotonic()
        if STOPPED_BY_INTERRUPT:
            output_handler.set_error("interrupted", runSet)
        output_handler.output_after_run_set(
            runSet, walltime=walltime_after - walltime_before
        )
        return

    sys.exit(f"Unknown --slurm-mode: {mode}")


def _results_dir_for_run_set(benchmark, runSet):
    """Deterministic results directory path for a run set."""
    scratchdir = benchmark.config.scratchdir
    bench_name = benchmark.name
    runset_name = runSet.real_name or f"runset_{runSet.index}"
    return os.path.join(scratchdir, f"slurm_{bench_name}", runset_name)


# ---------------------------------------------------------------------------
# Stage 1: Generate
# ---------------------------------------------------------------------------


def _stage_generate(runSet, benchmark, results_dir):
    """Create a self-contained SLURM job bundle in results_dir.

    The bundle contains:
      - manifest.json  — list of all tasks with command lines and limits
      - slurm_worker.py — the worker script (copied so the bundle is portable)
      - job.sh — sbatch script using only paths relative to the bundle dir
    """
    manifest = _create_manifest(runSet, benchmark)
    manifest_path = os.path.join(results_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    num_tasks = len(manifest["tasks"])
    if num_tasks == 0:
        return

    # Copy worker script into the bundle so it's self-contained
    worker_dest = os.path.join(results_dir, "slurm_worker.py")
    shutil.copy2(_WORKER_SCRIPT, worker_dest)

    # Generate the sbatch script with *relative* paths
    job_script = _generate_job_script(benchmark, num_tasks)
    job_script_path = os.path.join(results_dir, "job.sh")
    with open(job_script_path, "w") as f:
        f.write(job_script)
    os.chmod(job_script_path, 0o755)

    logging.info(
        "Generated SLURM job bundle with %d tasks in: %s",
        num_tasks,
        os.path.abspath(results_dir),
    )
    logging.info("Next steps:")
    logging.info("  1. Copy the bundle to your HPC login node")
    logging.info("  2. cd into the bundle directory and run:  sbatch job.sh")
    logging.info("  3. After all jobs finish, copy the bundle back")
    logging.info(
        "  4. Run again with --slurm-mode=collect to produce BenchExec output"
    )


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


def _generate_job_script(benchmark, num_tasks):
    """Generate a SLURM batch script using paths relative to the script dir.

    All paths in the script are relative to BUNDLE_DIR (the directory
    containing job.sh), so the bundle can be placed anywhere on the HPC.
    """
    lines = [
        "#!/bin/bash",
    ]

    # ---------- SBATCH directives ----------
    lines.append("#SBATCH --ntasks=1")

    timelimit = benchmark.rlimits.cputime
    walltime = benchmark.rlimits.walltime
    effective_limit = walltime or (timelimit * 2 if timelimit else None)
    if effective_limit:
        h = int(effective_limit // 3600)
        m = int((effective_limit % 3600) // 60)
        s = int(effective_limit % 60)
        lines.append(f"#SBATCH --time={h}:{m:02d}:{s:02d}")

    cpus = benchmark.rlimits.cpu_cores
    if cpus:
        lines.append(f"#SBATCH --cpus-per-task={cpus}")

    memory = benchmark.rlimits.memory
    if memory:
        lines.append(f"#SBATCH --mem={int(memory / 1_000_000)}M")

    lines.append("#SBATCH --threads-per-core=1")

    array_spec = f"0-{num_tasks - 1}"
    if benchmark.num_of_threads and benchmark.num_of_threads < num_tasks:
        array_spec += f"%{benchmark.num_of_threads}"
    lines.append(f"#SBATCH --array={array_spec}")

    lines.append(f"#SBATCH --job-name=benchexec_{benchmark.name}")
    lines.append("#SBATCH --output=slurm_%A_%a.out")

    # Extra sbatch args (embedded as directives)
    extra = getattr(benchmark.config, "slurm_sbatch_args", None)
    if extra:
        for arg in shlex.split(extra):
            if arg.startswith("--"):
                lines.append(f"#SBATCH {arg}")
            elif arg.startswith("-"):
                lines.append(f"#SBATCH {arg}")

    lines.append("")
    lines.append("# Auto-generated by BenchExec SLURM executor")
    lines.append("# All paths are relative to this script's directory.")
    lines.append('BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"')
    lines.append("")

    singularity = getattr(benchmark.config, "singularity", None)
    if singularity:
        lines.append(
            f"singularity exec"
            f" -B ./:/lower"
            f" --no-home"
            f' -B "$BUNDLE_DIR":"$BUNDLE_DIR"'
            f" {shlex.quote(singularity)}"
            f' python3 "$BUNDLE_DIR/slurm_worker.py"'
            f' "$BUNDLE_DIR/manifest.json"'
            f" $SLURM_ARRAY_TASK_ID"
            f' "$BUNDLE_DIR"'
        )
    else:
        lines.append(
            f'python3 "$BUNDLE_DIR/slurm_worker.py"'
            f' "$BUNDLE_DIR/manifest.json"'
            f" $SLURM_ARRAY_TASK_ID"
            f' "$BUNDLE_DIR"'
        )

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Stage 2: Collect
# ---------------------------------------------------------------------------


def _stage_collect(runSet, benchmark, output_handler, results_dir):
    """Read results from the results directory and produce BenchExec output."""
    manifest_path = os.path.join(results_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        raise BenchExecException(
            f"Manifest not found at {manifest_path}. "
            f"Run with --slurm-mode=generate first."
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
