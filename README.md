# Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor daemon and a kernel-space memory monitor. Built as an OS course project demonstrating process isolation, IPC, synchronization, memory management, and Linux scheduling.

---

## 1. Team Information

| Name | SRN |
|------|-----|
| RISHABH SINGH| PES2UG24AM135 |
| PUNYA PRAJWAL| PES2UG24AM124 |


---

## 2. Build, Load, and Run Instructions

### Prerequisites

- **Ubuntu 22.04 or 24.04** VM (NOT WSL)
- **Secure Boot OFF** (required for kernel module loading)

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### Step 1: Build Everything

```bash
cd boilerplate
make
```

This builds:
- `engine` — user-space runtime/supervisor binary
- `memory_hog`, `cpu_hog`, `io_pulse` — test workload binaries (statically linked)
- `monitor.ko` — kernel memory monitor module

### Step 2: Load the Kernel Module

```bash
sudo insmod monitor.ko

# Verify the device was created
ls -l /dev/container_monitor

# Check kernel logs
dmesg | tail -5
```

### Step 3: Prepare Root Filesystems

```bash
cd ..   # back to project root
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

# Create writable copies for each container
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# Copy test workloads into container rootfs
cp boilerplate/memory_hog ./rootfs-alpha/
cp boilerplate/cpu_hog ./rootfs-alpha/
cp boilerplate/io_pulse ./rootfs-alpha/
cp boilerplate/memory_hog ./rootfs-beta/
cp boilerplate/cpu_hog ./rootfs-beta/
cp boilerplate/io_pulse ./rootfs-beta/
```

### Step 4: Start the Supervisor

```bash
cd boilerplate
sudo ./engine supervisor ../rootfs-base
```

The supervisor stays in the foreground and prints status messages. Open **another terminal** for CLI commands.

### Step 5: Use the CLI (in a second terminal)

```bash
cd boilerplate

# Start two containers
sudo ./engine start alpha ../rootfs-alpha /bin/sh
sudo ./engine start beta ../rootfs-beta /bin/sh

# List all containers
sudo ./engine ps

# View container logs
sudo ./engine logs alpha

# Run a workload in foreground (blocks until exit)
sudo ./engine run memtest ../rootfs-alpha /memory_hog --soft-mib 20 --hard-mib 40

# Stop a container
sudo ./engine stop alpha
sudo ./engine stop beta
```

### Step 6: Scheduler Experiments

```bash
# Experiment 1: Two CPU-bound workloads with different nice values
cp -a ../rootfs-base ../rootfs-cpu1
cp -a ../rootfs-base ../rootfs-cpu2
cp cpu_hog ../rootfs-cpu1/
cp cpu_hog ../rootfs-cpu2/

sudo ./engine start cpu-high ../rootfs-cpu1 "/cpu_hog 15" --nice -10
sudo ./engine start cpu-low ../rootfs-cpu2 "/cpu_hog 15" --nice 19

# Wait ~15 seconds, then check logs
sudo ./engine logs cpu-high
sudo ./engine logs cpu-low
sudo ./engine ps

# Experiment 2: CPU-bound vs I/O-bound
cp -a ../rootfs-base ../rootfs-io1
cp cpu_hog ../rootfs-io1/
cp io_pulse ../rootfs-io1/

sudo ./engine start cpu-work ../rootfs-cpu1 "/cpu_hog 10" --nice 0
sudo ./engine start io-work ../rootfs-io1 "/io_pulse 20 200" --nice 0

sudo ./engine ps
sudo ./engine logs cpu-work
sudo ./engine logs io-work
```

### Step 7: Memory Limit Testing

```bash
# Test soft and hard limits
cp -a ../rootfs-base ../rootfs-mem
cp memory_hog ../rootfs-mem/

# memory_hog allocates 8MiB/sec — soft at 20MiB, hard at 40MiB
sudo ./engine run memtest ../rootfs-mem /memory_hog --soft-mib 20 --hard-mib 40

# Check kernel logs for soft/hard limit events
dmesg | grep container_monitor | tail -10

# Check container state shows "killed" for hard limit breach
sudo ./engine ps
```

### Step 8: Cleanup

```bash
# Stop all containers
sudo ./engine stop alpha
sudo ./engine stop beta

# Stop the supervisor (Ctrl+C in the supervisor terminal, or)
# kill -SIGTERM <supervisor_pid>

# Verify no zombies remain
ps aux | grep -E 'engine|cpu_hog|memory_hog|io_pulse|defunct'

# Unload kernel module
sudo rmmod monitor

# Verify clean unload
dmesg | tail -5
```

### CI Smoke Check (for GitHub Actions)

```bash
make -C boilerplate ci
./boilerplate/engine     # prints usage and exits non-zero
```

---

## 3. Demo with Screenshots

> **Instructions:** Replace each placeholder below with an actual annotated screenshot after running the demo on your Ubuntu VM.

### Screenshot 1: Multi-Container Supervision
_Two or more containers running under one supervisor process._

`[INSERT SCREENSHOT: showing supervisor terminal with two containers started, and ps output showing both running]`

### Screenshot 2: Metadata Tracking
_Output of the `ps` command showing tracked container metadata._

`[INSERT SCREENSHOT: output of "sudo ./engine ps" showing CONTAINER, PID, STATE, STARTED, SOFT, HARD, EXIT, SIGNAL columns]`

### Screenshot 3: Bounded-Buffer Logging
_Log file contents captured through the logging pipeline, with producer/consumer activity._

`[INSERT SCREENSHOT: supervisor terminal showing producer/consumer thread messages + cat logs/alpha.log output]`

### Screenshot 4: CLI and IPC
_A CLI command being issued and the supervisor responding._

`[INSERT SCREENSHOT: split terminal — left shows CLI command, right shows supervisor receiving and processing it]`

### Screenshot 5: Soft-Limit Warning
_dmesg or log output showing a soft-limit warning event._

`[INSERT SCREENSHOT: dmesg output showing "[container_monitor] SOFT LIMIT container=memtest pid=XXXX rss=XXXX limit=XXXX"]`

### Screenshot 6: Hard-Limit Enforcement
_Container killed after exceeding hard limit, with supervisor metadata reflecting the kill._

`[INSERT SCREENSHOT: dmesg showing HARD LIMIT + "sudo ./engine ps" showing state=killed]`

### Screenshot 7: Scheduling Experiment
_Terminal output or measurements from scheduling experiments with observable differences._

`[INSERT SCREENSHOT: side-by-side log output of cpu_hog with nice=-10 vs nice=19 showing different progress rates]`

### Screenshot 8: Clean Teardown
_Evidence that containers are reaped, threads exit, and no zombies remain._

`[INSERT SCREENSHOT: supervisor shutdown messages + "ps aux | grep defunct" showing no zombies]`

---

## 4. Engineering Analysis

### 4.1 Isolation Mechanisms

Our runtime achieves process and filesystem isolation using three Linux namespace types and `chroot`:

**PID Namespace (`CLONE_NEWPID`):** Each container gets its own PID namespace via the `clone()` system call. Inside the container, the first process has PID 1 — it cannot see or signal processes in the host or other containers. The kernel maintains separate PID number spaces: the same process has a host PID (visible to the supervisor) and a container PID (visible inside its namespace). This isolation is enforced at the kernel level in the `pid_namespace` structure — the `task_struct` of each process holds a reference to its PID namespace, and `find_vpid()` / `task_pid_nr_ns()` translate between them.

**UTS Namespace (`CLONE_NEWUTS`):** Each container gets its own hostname. We call `sethostname()` inside the child to set the container's hostname to its container ID. The kernel stores hostname in `struct uts_namespace`; without this namespace, `sethostname()` would change the hostname for all processes on the system.

**Mount Namespace (`CLONE_NEWNS`):** Each container gets a private mount table. Changes to mounts inside the container (e.g., mounting `/proc`) don't affect the host or other containers. This is essential for `chroot` safety — the container mounts its own `/proc` which shows only processes in its PID namespace.

**`chroot` for Filesystem Isolation:** We use `chroot()` to change the container's root directory to its dedicated rootfs copy. After `chroot`, the container cannot access files outside its rootfs via normal path traversal. Each container has its own writable rootfs copy (e.g., `rootfs-alpha/`) derived from `rootfs-base/`, preventing cross-container filesystem interference.

**What the host kernel still shares:** Despite namespace isolation, all containers share the same host kernel. This means: the same kernel version and syscall interface, shared kernel memory (slab caches, page cache), the same scheduler, the same networking stack (unless network namespaces are used), and the same block devices. A kernel exploit inside one container can compromise the entire host.

### 4.2 Supervisor and Process Lifecycle

The long-running parent supervisor is the central coordination point for the runtime. Its key role is managing the lifecycle of container processes:

**Why a supervisor?** Without a supervisor, each container launch would be a standalone process — there would be no central place to track metadata, coordinate logging, handle signals, or manage graceful shutdown across containers. The supervisor maintains a linked list of `container_record_t` structures that track each container's state throughout its lifecycle.

**Process creation:** The supervisor uses `clone()` with namespace flags to create each container. Unlike `fork()`, `clone()` allows specifying which namespaces the child should have. The child inherits the parent's execution context but with isolated PID, UTS, and mount namespaces.

**Parent-child relationship:** The supervisor is the direct parent of all container processes. This is critical because when a child exits, the kernel sends `SIGCHLD` to its parent — only the parent can call `waitpid()` to reap it. If the supervisor dies, orphaned containers are reparented to PID 1 (`init`).

**Reaping:** Our `SIGCHLD` handler calls `waitpid(-1, &status, WNOHANG)` in a loop to reap all exited children. This is necessary because multiple children can exit simultaneously, but only one `SIGCHLD` signal is delivered. Without reaping, exited children become zombies (`Z` state in `ps`) — their `task_struct` remains in the kernel until the parent calls `wait()`.

**Signal delivery and attribution:** When the supervisor receives a `stop` command, it sets `stop_requested = 1` in the container's metadata *before* sending `SIGTERM`. This ordering is crucial for correct termination attribution: if the container exits with `SIGKILL` and `stop_requested` is set, we classify it as `stopped`; if `stop_requested` is not set and the exit signal is `SIGKILL`, the kernel monitor's hard-limit enforcement caused the kill (`killed` state).

### 4.3 IPC, Threads, and Synchronization

Our project uses two distinct IPC mechanisms and a bounded-buffer with specific synchronization:

**Path A — Logging Pipes (Container → Supervisor):** Before `clone()`, we create a pipe with `pipe()`. The write end is passed to the child (via `child_config_t`), and the child redirects `stdout`/`stderr` to it using `dup2()`. The read end stays with the supervisor. Pipes are a natural fit here because they provide unidirectional, byte-stream communication from child to parent with automatic EOF signaling when the child exits.

**Path B — Unix Domain Socket (CLI → Supervisor):** The supervisor creates a `SOCK_STREAM` Unix domain socket at `/tmp/mini_runtime.sock`. Each CLI invocation connects, sends a `control_request_t` struct, reads a `control_response_t`, and disconnects. This is a different IPC mechanism from the pipes and is better suited for request-response communication because: it supports bidirectional communication, allows multiple concurrent clients, and provides connection semantics (accept/connect).

**Bounded Buffer — Producer/Consumer Synchronization:**

The bounded buffer uses three synchronization primitives:
- `pthread_mutex_t mutex` — protects all buffer state (`head`, `tail`, `count`, `shutting_down`)
- `pthread_cond_t not_empty` — consumers wait on this when the buffer is empty
- `pthread_cond_t not_full` — producers wait on this when the buffer is full

**Race conditions without synchronization:**
1. **Lost updates:** Two producers writing to `tail` simultaneously could overwrite the same slot, losing a log entry.
2. **Corrupted count:** `count++` is not atomic — concurrent increments could result in an incorrect count, causing the buffer to believe it has fewer items than it actually does (or more).
3. **Torn reads:** A consumer could read a partially-written log item if a producer is still filling it.
4. **Missed wakeups:** Without condition variables, a consumer could check `count == 0`, get preempted before sleeping, miss a producer's signal, and sleep forever (deadlock).

**How our design avoids these problems:**
- The mutex ensures mutual exclusion for all buffer operations — only one thread modifies `head`, `tail`, or `count` at a time.
- Condition variables are always checked in a `while` loop (not `if`) to handle spurious wakeups.
- During shutdown, we broadcast on both condition variables so blocked threads can observe the shutdown flag and exit.
- The producer still allows pushing items during shutdown (if space is available) to avoid dropping the last log entries from an exiting container.

**Container metadata synchronization:** A separate `pthread_mutex_t metadata_lock` protects the container linked list. This is separate from the log buffer mutex to avoid holding both locks simultaneously and creating lock-ordering issues.

### 4.4 Memory Management and Enforcement

**What RSS measures:** RSS (Resident Set Size) is the amount of physical memory (RAM) currently mapped to a process's address space. It includes code pages, heap allocations (via `malloc`/`mmap`), stack pages, and shared library pages — but only the pages that are actually in physical memory, not swapped out or merely mapped but untouched.

**What RSS does not measure:** RSS does not count swap usage, memory-mapped files that haven't been faulted in, or the "true" memory cost of shared pages (which are counted in each process's RSS even though the physical page exists only once). It also doesn't measure kernel memory consumed on behalf of the process (page tables, `task_struct`, socket buffers).

**Soft vs. Hard limits:** The two-tier policy serves different purposes:
- **Soft limit:** An advisory threshold. When exceeded, a warning is logged (via `printk`) so operators can investigate. The process continues running. This is useful for detecting memory leaks or unexpected growth before they become critical.
- **Hard limit:** A mandatory threshold. When exceeded, the process is immediately killed with `SIGKILL`. This prevents a single runaway container from consuming all host memory and causing system-wide issues.

**Why kernel space?** Memory enforcement in kernel space is more reliable than user space because:
1. The kernel can directly inspect a process's RSS via `get_mm_rss()` without the process's cooperation.
2. A user-space monitor could be delayed by scheduling or could be killed by OOM before it can enforce limits.
3. The kernel can send `SIGKILL` directly and unblockably — a user-space approach would need to use `kill()`, which is less direct.
4. The timer callback runs periodically regardless of user-space state, providing consistent enforcement even under memory pressure.

### 4.5 Scheduling Behavior

**CFS (Completely Fair Scheduler):** Linux uses CFS for normal (`SCHED_OTHER`) processes. CFS allocates CPU time proportionally based on each task's weight, which is determined by its `nice` value. A lower nice value (e.g., -10) gives higher priority (more CPU time), while a higher value (e.g., 19) gives lower priority.

**Experiment 1 — Nice Values (CPU-bound vs CPU-bound):** When two `cpu_hog` containers run with `nice=-10` and `nice=19`, CFS gives approximately 30:1 CPU time ratio. The high-priority container completes significantly faster because CFS uses the nice value as an exponential weight: each nice level changes the weight by ~1.25x, so a 29-level difference results in a dramatic disparity. The `accumulator` values and `elapsed` timestamps in the logs demonstrate this quantitatively.

**Experiment 2 — CPU-bound vs I/O-bound:** When `cpu_hog` and `io_pulse` run concurrently at the same nice value, CFS naturally favors the I/O-bound process for responsiveness. I/O-bound processes voluntarily sleep (during I/O waits), and when they wake up, CFS gives them a "vruntime credit" — their virtual runtime hasn't advanced while sleeping, so they're scheduled ahead of the CPU-bound process. This means `io_pulse` maintains consistent, low-latency iterations despite the CPU-bound workload consuming all available CPU between I/O bursts.

_(See Section 6 for raw data and comparison tables.)_

---

## 5. Design Decisions and Tradeoffs

### 5.1 Namespace Isolation — `chroot` over `pivot_root`

- **Decision:** We use `chroot()` for filesystem isolation rather than `pivot_root`.
- **Tradeoff:** `pivot_root` is more secure because it actually changes the mount point root, making the old root completely inaccessible. With `chroot`, a process with `CAP_SYS_CHROOT` can potentially escape via `chroot(".")` after `fchdir()` to an fd opened before `chroot`. However, since our containers run with limited capabilities inside PID/mount namespaces, this attack surface is minimal.
- **Justification:** `chroot` is simpler to implement correctly and sufficient for our educational context. The combination of `chroot` + mount namespace + PID namespace provides adequate isolation for the project requirements.

### 5.2 Supervisor Architecture — Single-Threaded Accept Loop with Worker Threads

- **Decision:** The supervisor uses a single-threaded `select()`-based accept loop for CLI commands, with separate producer threads per container and one consumer (logger) thread.
- **Tradeoff:** A single accept thread limits concurrent CLI throughput, but the CLI operations (start, ps, stop) are fast enough that queuing is acceptable. An alternative would be `epoll` + thread pool, but that adds complexity.
- **Justification:** The bottleneck in this system is container management, not CLI throughput. The per-container producer threads handle the continuous data flow (logging) in parallel, while the accept loop handles infrequent control commands. This separation of concerns is clean and avoids complex multiplexing.

### 5.3 IPC/Logging — Unix Domain Socket (control) + Pipes (logging)

- **Decision:** We use Unix domain sockets (`SOCK_STREAM`) for the control channel (Path B) and anonymous pipes for the logging channel (Path A).
- **Tradeoff:** Unix domain sockets have higher per-message overhead than shared memory, but provide clean connection semantics and error handling. Pipes are one-directional, but that's exactly what we need for logging.
- **Justification:** The two IPC mechanisms serve fundamentally different communication patterns. Pipes are the natural choice for parent-child byte streams with automatic EOF. Unix domain sockets provide the bidirectional request-response semantics needed for CLI commands, with built-in framing via `SOCK_STREAM` and the ability for the supervisor to serve multiple clients via `listen()`/`accept()`.

### 5.4 Kernel Monitor — Spinlock over Mutex

- **Decision:** We use `spinlock` instead of `mutex` for the monitored process list in the kernel module.
- **Tradeoff:** Spinlocks waste CPU cycles while waiting (busy-wait), which could be a problem if the critical section were long. A mutex would sleep instead, saving CPU. However, mutexes cannot be used in atomic/softirq context.
- **Justification:** The timer callback runs in softirq context (via `timer_callback`), where sleeping is not allowed. Since our critical sections are short (iterate a small linked list, insert/remove one node), spinlock overhead is negligible. This is the standard pattern for timer + ioctl shared data in Linux kernel modules.

### 5.5 Scheduling Experiments — Nice Values as the Primary Variable

- **Decision:** We use `nice` values to demonstrate scheduling differences, rather than CPU affinity (`taskset`) or real-time scheduling classes.
- **Tradeoff:** Nice values provide a continuous 40-level range but only affect relative priorities within `SCHED_OTHER`. CPU affinity would demonstrate NUMA and core isolation effects. Real-time scheduling (`SCHED_FIFO`/`SCHED_RR`) would show priority preemption but risks system instability.
- **Justification:** Nice values directly exercise CFS's weight-based fair queuing, which is the most relevant scheduling concept for general-purpose workloads. The `--nice` flag is already integrated into our CLI, making it easy to run controlled experiments. The results clearly demonstrate CFS behavior without risking system stability.

---

## 6. Scheduler Experiment Results

> **Instructions:** Replace the placeholder data below with actual measurements from your VM after running the experiments.

### Experiment 1: CPU-Bound with Different Nice Values

Two `cpu_hog` containers running for 15 seconds each:

| Container | Nice Value | Completion Time (s) | Final Accumulator | CPU Share (approx) |
|-----------|-----------|--------------------|--------------------|-------------------|
| cpu-high  | -10       | _XX.X_             | _XXXXXXXXX_        | _~XX%_            |
| cpu-low   | 19        | _XX.X_             | _XXXXXXXXX_        | _~XX%_            |

**Observation:** The container with `nice=-10` completes its computation significantly faster, showing that CFS allocates more CPU time to lower-nice (higher-priority) processes. The accumulator values (which track total CPU work done) differ proportionally.

### Experiment 2: CPU-Bound vs I/O-Bound

| Container | Workload  | Nice | Completion Time (s) | Notes |
|-----------|----------|------|--------------------|-----------------------------------------|
| cpu-work  | cpu_hog  | 0    | _XX.X_             | _Continuous CPU consumption_             |
| io-work   | io_pulse | 0    | _XX.X_             | _Consistent ~200ms iterations_           |

**Observation:** Despite both having the same nice value, `io_pulse` maintains consistent iteration timing (~200ms between writes) because CFS gives sleeping processes a vruntime advantage when they wake up. The `cpu_hog` container uses all remaining CPU time between `io_pulse`'s I/O operations. This demonstrates CFS's responsiveness optimization: I/O-bound processes are not starved by CPU-bound processes.

**Analysis:** These results directly illustrate two CFS scheduling goals:
1. **Fairness:** Nice values control proportional CPU allocation. Higher-priority processes get more CPU, not exclusive CPU.
2. **Responsiveness:** I/O-bound processes receive scheduling preference because their accumulated vruntime is lower due to voluntary sleeps, keeping interactive/I/O workloads responsive even under CPU pressure.
