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

This extension allows BenchExec benchmarks to run on SLURM clusters via a **two-stage offline workflow**:

1. **Generate** (local machine): Create a self-contained SLURM job bundle — a directory with an sbatch script, a manifest, and a worker script.
2. **Collect** (local machine): After the jobs finish on the HPC and the results are copied back, parse them into standard BenchExec output.

The HPC interaction (transfer, `sbatch`, waiting) is done manually by the user, so **no SLURM commands are executed on the local machine**.

Resource measurement uses cgroup v2 accounting from the cgroups created by SLURM itself — no custom cgroup creation needed. This works with modern SLURM versions that forbid user-created cgroups.

In case of problems, please tag [Levente Bajczi](https://github.com/leventeBajczi) (@leventeBajczi) in an [issue](https://github.com/sosy-lab/benchexec/issues/new/choose).

## Requirements

* Python 3.8+ on all nodes (local and HPC)
* [SLURM](https://slurm.schedmd.com/documentation.html) on the HPC cluster (tested with 22.x and 23.x)
* cgroup v2 enabled on HPC compute nodes (standard on modern kernels)
* [Singularity](https://docs.sylabs.io/guides/latest/user-guide/) (optional, tested with 4.x)

## Workflow

### Step 1: Generate the job bundle (local)

```bash
python3 contrib/slurm-benchmark.py \
    --slurm \
    --slurm-mode=generate \
    --scratchdir ./scratch \
    benchmark.xml
```

This creates a self-contained directory under `./scratch/slurm_<benchmark>/<runset>/` containing:

```
manifest.json       # All task command lines and resource limits
slurm_worker.py     # Worker script (copied, no external dependencies)
job.sh              # sbatch script (uses relative paths only)
```

### Step 2: Transfer to HPC

```bash
scp -r ./scratch/slurm_<benchmark>/ hpc-login:~/jobs/
```

Also transfer any tool binaries, input files, etc. that the commands in the manifest reference.

### Step 3: Submit on HPC

```bash
ssh hpc-login
cd ~/jobs/slurm_<benchmark>/<runset>/
sbatch job.sh
```

Monitor with `squeue -u $USER`. Each array task writes its result files (`<index>.json`, `<index>.log`) into the same directory.

### Step 4: Copy results back (local)

```bash
scp -r hpc-login:~/jobs/slurm_<benchmark>/ ./scratch/
```

### Step 5: Collect results (local)

```bash
python3 contrib/slurm-benchmark.py \
    --slurm \
    --slurm-mode=collect \
    --scratchdir ./scratch \
    benchmark.xml
```

This reads the per-task JSON results and log files, copies logs to the standard BenchExec log folder, and produces XML output. The result is indistinguishable from a local BenchExec run (except for per-node metadata).

## Options

| Option | Description |
|---|---|
| `--slurm` | Use SLURM executor (without this, runs locally) |
| `--slurm-mode {generate,collect}` | Which stage to run (default: `generate`) |
| `--singularity <path.sif>` | Path to Singularity container image |
| `--scratchdir <path>` | Directory for the job bundle (default: `./`) |
| `--slurm-sbatch-args "<args>"` | Extra sbatch directives (e.g., `"--partition=compute --qos=normal"`) |
| `-N <N>` | Maximum number of concurrent SLURM array tasks |

## Singularity Support

```bash
python3 contrib/slurm-benchmark.py \
    --slurm \
    --singularity container.sif \
    --scratchdir ./scratch \
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

The generated `job.sh` wraps the worker call in `singularity exec` with:
- `-B ./:/lower` — bind CWD as read-only lower layer
- `--no-home` — don't mount home directory
- `-B <bundle_dir>:<bundle_dir>` — make results dir writable

## Resource Monitoring

Each SLURM array task runs `slurm_worker.py`, which:

1. Detects its own cgroup via `/proc/self/cgroup`
2. Reads resource usage from cgroup v2 files:

| Metric | Source | Notes |
|---|---|---|
| CPU time | `cpu.stat` (`usage_usec`) | Measured via cgroup, not SLURM accounting |
| Wall time | Monotonic clock | Measured in the worker process |
| Peak memory | `memory.peak` or polled `memory.current` | `memory.peak` requires Linux 5.19+ |
| OOM detection | `memory.events` (`oom_kill`) | Sets `terminationreason=memory` |

3. Collects system info: hostname, CPU model, core count, total memory
4. Writes a JSON result file and a log file (with BenchExec's 6-line header format)

## Bundle Directory Structure

After generate + HPC execution:
```
<scratchdir>/slurm_<benchmark>/
  <runset>/
    manifest.json         # Task descriptions
    slurm_worker.py       # Self-contained worker (no BenchExec dependency)
    job.sh                # sbatch script (relative paths only)
    0.json, 1.json, ...   # Per-task result files (written by workers)
    0.log, 1.log, ...     # Per-task log files (written by workers)
    slurm_<jobid>_<taskid>.out  # SLURM stdout/stderr
```

## Limitations

1. **cgroup v2 required** on compute nodes. Systems with only cgroup v1 are not supported.
2. **Only CPU time, wall time, and memory** are monitored. No file hierarchy limits.
3. **Tool binaries and inputs must be available on the HPC** — the manifest records command lines as-is from the local generation.
4. **System info is per-node** — each task reports its own node's info. No single "system" in the XML header.
5. **`--threads-per-core=1` is always set** — hyperthreading control is limited to this.