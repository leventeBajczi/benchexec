<!--
This file is part of BenchExec, a framework for reliable benchmarking:
https://github.com/sosy-lab/benchexec

SPDX-FileCopyrightText: 2021 Dirk Beyer <https://www.sosy-lab.org>
SPDX-FileCopyrightText: 2024-2026 Levente Bajczi
SPDX-FileCopyrightText: Critical Systems Research Group
SPDX-FileCopyrightText: Budapest University of Technology and Economics <https://www.ftsrg.mit.bme.hu>

SPDX-License-Identifier: Apache-2.0
-->
# BenchExec Extension for Benchmarking via SLURM

This extension allows BenchExec benchmarks to run on SLURM clusters via a **two-stage workflow**:

1. **Submit**: Generate and submit a SLURM array job (one task per BenchExec run). Each task monitors its own resource usage via the cgroup created by SLURM.
2. **Collect**: Parse the results and produce standard BenchExec output (XML, tables, log files).

This approach works with modern SLURM versions that forbid user-created cgroups by relying entirely on SLURM's own cgroup accounting (cgroup v2).

In case of problems, please tag [Levente Bajczi](https://github.com/leventeBajczi) (@leventeBajczi) in an [issue](https://github.com/sosy-lab/benchexec/issues/new/choose).

## Preliminaries

* [SLURM](https://slurm.schedmd.com/documentation.html) — open-source HPC job scheduler.
* [Singularity](https://docs.sylabs.io/guides/latest/user-guide/) — optional containerization for reproducible environments.

## Requirements

* SLURM (tested with 22.x and 23.x)
* Python 3.8+ on all nodes
* cgroup v2 enabled on compute nodes (standard on modern kernels)
* Singularity (optional, tested with 4.x)
* A shared filesystem accessible from all nodes (for the scratchdir)

## Usage

### One-shot mode (submit + wait + collect)

```bash
python3 $BENCHEXEC_FOLDER/contrib/slurm-benchmark.py \
    --slurm \
    --scratchdir /shared/scratch \
    benchmark.xml
```

This submits the SLURM array job, waits for all tasks to finish, then collects results.

### Two-stage mode

**Stage 1 — Submit jobs:**
```bash
python3 $BENCHEXEC_FOLDER/contrib/slurm-benchmark.py \
    --slurm \
    --slurm-mode=submit \
    --scratchdir /shared/scratch \
    benchmark.xml
```

**Stage 2 — Collect results** (after all SLURM jobs have finished):
```bash
python3 $BENCHEXEC_FOLDER/contrib/slurm-benchmark.py \
    --slurm \
    --slurm-mode=collect \
    --scratchdir /shared/scratch \
    benchmark.xml
```

### Options

| Option | Description |
|---|---|
| `--slurm` | Use SLURM executor (without this, runs locally) |
| `--slurm-mode {submit,collect,both}` | Which stage to run (default: `both`) |
| `--singularity <path.sif>` | Path to Singularity container image |
| `--scratchdir <path>` | Shared directory for intermediate results (must be accessible from all nodes) |
| `--slurm-sbatch-args "<args>"` | Extra arguments passed to `sbatch` (e.g., `"--partition=compute --qos=normal"`) |
| `-N <N>` | Maximum number of concurrent SLURM tasks |

### With Singularity

```bash
python3 $BENCHEXEC_FOLDER/contrib/slurm-benchmark.py \
    --slurm \
    --singularity container.sif \
    --scratchdir /shared/scratch \
    benchmark.xml
```

Example Singularity definition:
```singularity
BootStrap: docker
From: ubuntu:22.04

%post
apt -y update
apt -y install python3 openjdk-17-jre-headless libgomp1
```

Build with: `singularity build [--remote|--fakeroot] --fix-perms container.sif container.def`

## How It Works

### Stage 1: Submit

1. For each run set, a **manifest file** (JSON) is created listing every run's command line and resource limits.
2. A SLURM **array job** is submitted (`sbatch --array=0-N`). Each array task:
   - Reads its entry from the manifest
   - Executes the tool command
   - Detects its own cgroup (via `/proc/self/cgroup`)
   - Reads resource usage from cgroup v2 files:
     - CPU time from `cpu.stat` (`usage_usec`)
     - Peak memory from `memory.peak` (Linux 5.19+) or polled from `memory.current`
     - OOM detection from `memory.events`
   - Measures wall time via monotonic clock
   - Collects system info (hostname, CPU model, memory)
   - Writes a JSON result file and a log file (with BenchExec's 6-line header format)

### Stage 2: Collect

1. Reads back each task's JSON result and log file.
2. Copies log files to the standard BenchExec log folder structure.
3. Calls `run.set_result()` with the collected metrics.
4. Produces standard BenchExec XML output, indistinguishable from local runs (except for per-node metadata).

## Resource Monitoring

| Metric | Source | Notes |
|---|---|---|
| CPU time | `cpu.stat` (`usage_usec`) | Measured via cgroup, not SLURM accounting |
| Wall time | Monotonic clock | Measured in the worker process |
| Peak memory | `memory.peak` or polled `memory.current` | `memory.peak` requires Linux 5.19+ |
| OOM detection | `memory.events` (`oom_kill`) | Sets `terminationreason=memory` |

## Limitations

1. **cgroup v2 required**: The worker reads cgroup v2 files. Systems with only cgroup v1 are not supported.
2. **No file hierarchy limits**: Only CPU time, wall time, and memory are monitored.
3. **Shared filesystem required**: The scratchdir must be accessible from all compute nodes.
4. **System info is per-node**: Each task reports its own node's info; there is no single "system" in the XML header.
5. **No hyperthreading control beyond `--threads-per-core=1`**: The executor always requests one thread per core from SLURM.
6. **Cancellation delay**: Interrupting (Ctrl+C) in `both` mode cancels the SLURM job, but running tasks may take time to terminate.

## Directory Structure

After submit, the scratchdir contains:
```
<scratchdir>/slurm_results_<benchmark>/
  <runset>/
    manifest.json         # Task descriptions
    job.sh                # Generated SLURM batch script
    job_id                # SLURM job ID
    0.json, 1.json, ...   # Per-task result files
    0.log, 1.log, ...     # Per-task log files
    slurm_<jobid>_<taskid>.out  # SLURM stdout/stderr
```

The final BenchExec output is written to the standard `results/` directory.