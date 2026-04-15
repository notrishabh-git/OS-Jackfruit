/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implements:
 *   - Long-running supervisor daemon with Unix domain socket IPC
 *   - Multi-container lifecycle management via clone() with namespaces
 *   - Bounded-buffer logging pipeline (producer/consumer threads)
 *   - CLI client (start, run, ps, logs, stop)
 *   - Signal handling (SIGCHLD reap, SIGINT/SIGTERM orderly shutdown)
 *   - Integration with kernel memory monitor via ioctl
 *   - Resource cleanup (threads, fds, memory, children)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <sys/resource.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define MAX_CONTAINERS 64
#define STOP_GRACE_SEC 5

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;         /* set before sending stop signal */
    int pipe_read_fd;           /* read-end of stdout/stderr pipe */
    pthread_t producer_thread;  /* producer thread handle */
    int producer_active;        /* whether producer thread is running */
    int nice_value;
    int run_client_fd;          /* client fd for 'run' command (-1 if start) */
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
} supervisor_ctx_t;

/* Producer thread argument */
typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd;
} producer_arg_t;

/* Global supervisor context pointer (for signal handlers) */
static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* =====================================================
 * Bounded Buffer Implementation (Task 3)
 * ===================================================== */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * Producer-side insertion into the bounded buffer.
 *
 * Blocks while the buffer is full (using condition variable not_full).
 * Returns 0 on success, -1 if shutdown is in progress.
 * Wakes consumers via not_empty after insertion.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while the buffer is full and we're not shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    if (buffer->shutting_down) {
        /* Even during shutdown, allow pushing remaining items
         * so no log lines are lost. Only reject if truly full. */
        if (buffer->count == LOG_BUFFER_CAPACITY) {
            pthread_mutex_unlock(&buffer->mutex);
            return -1;
        }
    }

    /* Insert at tail */
    memcpy(&buffer->items[buffer->tail], item, sizeof(*item));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake one consumer */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * Consumer-side removal from the bounded buffer.
 *
 * Blocks while the buffer is empty (using condition variable not_empty).
 * Returns 0 on success with item populated.
 * Returns -1 when shutdown is in progress AND the buffer is empty (drain complete).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is empty */
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    /* If shutting down and buffer is empty, signal drain complete */
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Remove from head */
    memcpy(item, &buffer->items[buffer->head], sizeof(*item));
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake one producer */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * Logging consumer thread (Task 3).
 *
 * Removes log chunks from the bounded buffer and writes them
 * to per-container log files (logs/<container_id>.log).
 * Exits cleanly when shutdown begins and all pending work is drained.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    fprintf(stderr, "[logger] Consumer thread started\n");

    while (1) {
        int rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc < 0) {
            /* Shutdown complete and buffer drained */
            break;
        }

        /* Write to per-container log file */
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            ssize_t written = 0;
            while (written < (ssize_t)item.length) {
                ssize_t n = write(fd, item.data + written, item.length - written);
                if (n <= 0) break;
                written += n;
            }
            close(fd);
        } else {
            fprintf(stderr, "[logger] Failed to open log file: %s: %s\n",
                    log_path, strerror(errno));
        }
    }

    fprintf(stderr, "[logger] Consumer thread exiting\n");
    return NULL;
}

/*
 * Producer thread function.
 *
 * Reads from a container's pipe (stdout/stderr) and pushes
 * log chunks into the bounded buffer. Exits when the pipe
 * returns EOF (container exited) or read error.
 */
void *producer_thread_fn(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    fprintf(stderr, "[producer:%s] Started reading pipe fd=%d\n",
            parg->container_id, parg->pipe_fd);

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, parg->container_id,
                sizeof(item.container_id) - 1);

        n = read(parg->pipe_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0) {
            /* EOF or error — container exited or pipe closed */
            if (n < 0 && errno != EINTR) {
                fprintf(stderr, "[producer:%s] read error: %s\n",
                        parg->container_id, strerror(errno));
            }
            break;
        }

        item.length = (size_t)n;
        item.data[n] = '\0';

        if (bounded_buffer_push(&parg->ctx->log_buffer, &item) < 0) {
            fprintf(stderr, "[producer:%s] Buffer shutdown, dropping data\n",
                    parg->container_id);
            break;
        }
    }

    close(parg->pipe_fd);
    fprintf(stderr, "[producer:%s] Exiting\n", parg->container_id);

    /* Mark producer as inactive */
    pthread_mutex_lock(&parg->ctx->metadata_lock);
    container_record_t *rec = parg->ctx->containers;
    while (rec) {
        if (strcmp(rec->id, parg->container_id) == 0) {
            rec->producer_active = 0;
            rec->pipe_read_fd = -1;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&parg->ctx->metadata_lock);

    free(parg);
    return NULL;
}

/* =====================================================
 * Container Child Entrypoint (Task 1)
 * ===================================================== */

/*
 * Clone child entrypoint.
 *
 * Runs inside the new PID/UTS/mount namespaces.
 * Sets up filesystem isolation (chroot), mounts /proc,
 * redirects stdout/stderr to the logging pipe,
 * sets nice value, and exec's the requested command.
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set hostname (UTS namespace) */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("sethostname");
        _exit(1);
    }

    /* chroot into the container's rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        _exit(1);
    }

    if (chdir("/") != 0) {
        perror("chdir /");
        _exit(1);
    }

    /* Mount /proc inside the container so ps works */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        /* Non-fatal: continue anyway */
    }

    /* Mount /tmp for workloads that need it */
    mkdir("/tmp", 01777);
    mount("tmpfs", "/tmp", "tmpfs", 0, NULL);

    /* Redirect stdout and stderr to the logging pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        if (cfg->log_write_fd != STDOUT_FILENO &&
            cfg->log_write_fd != STDERR_FILENO) {
            close(cfg->log_write_fd);
        }
    }

    /* Set nice value if specified */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0) {
            /* Non-fatal */
            fprintf(stderr, "nice(%d) failed: %s\n",
                    cfg->nice_value, strerror(errno));
        }
    }

    /* Parse command — support simple space-separated args */
    char *argv_child[64];
    int argc_child = 0;
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, sizeof(cmd_copy) - 1);
    cmd_copy[sizeof(cmd_copy) - 1] = '\0';

    char *tok = strtok(cmd_copy, " ");
    while (tok && argc_child < 63) {
        argv_child[argc_child++] = tok;
        tok = strtok(NULL, " ");
    }
    argv_child[argc_child] = NULL;

    if (argc_child == 0) {
        fprintf(stderr, "No command specified\n");
        _exit(1);
    }

    /* Execute the command */
    execvp(argv_child[0], argv_child);
    perror("execvp");
    _exit(1);
}

/* =====================================================
 * Monitor Integration
 * ===================================================== */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0) {
        perror("ioctl MONITOR_REGISTER");
        return -1;
    }

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    if (monitor_fd < 0)
        return -1;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0) {
        perror("ioctl MONITOR_UNREGISTER");
        return -1;
    }

    return 0;
}

/* =====================================================
 * Supervisor: Container Lifecycle (Task 1 & 2)
 * ===================================================== */

/*
 * Find a container record by ID.
 * Caller must hold metadata_lock.
 */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0)
            return rec;
        rec = rec->next;
    }
    return NULL;
}

/*
 * Launch a container: clone with namespaces, set up pipes, register
 * with kernel monitor, start producer thread.
 */
static int launch_container(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            control_response_t *resp,
                            int client_fd)
{
    container_record_t *rec;
    int pipe_fds[2]; /* [0]=read, [1]=write */
    pid_t child_pid;
    char *stack;
    child_config_t child_cfg;
    producer_arg_t *parg;

    /* Check for duplicate ID */
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' already exists", req->container_id);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Check rootfs exists */
    struct stat st;
    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Rootfs '%s' does not exist or is not a directory", req->rootfs);
        return -1;
    }

    /* Create pipe for container stdout/stderr → supervisor */
    if (pipe(pipe_fds) != 0) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Failed to create pipe: %s", strerror(errno));
        return -1;
    }

    /* Prepare child config */
    memset(&child_cfg, 0, sizeof(child_cfg));
    strncpy(child_cfg.id, req->container_id, sizeof(child_cfg.id) - 1);
    /* Use absolute path for rootfs */
    if (req->rootfs[0] == '/') {
        strncpy(child_cfg.rootfs, req->rootfs, sizeof(child_cfg.rootfs) - 1);
    } else {
        if (realpath(req->rootfs, child_cfg.rootfs) == NULL) {
            strncpy(child_cfg.rootfs, req->rootfs, sizeof(child_cfg.rootfs) - 1);
        }
    }
    strncpy(child_cfg.command, req->command, sizeof(child_cfg.command) - 1);
    child_cfg.nice_value = req->nice_value;
    child_cfg.log_write_fd = pipe_fds[1];

    /* Allocate stack for clone */
    stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Failed to allocate stack");
        return -1;
    }

    /* clone with PID, UTS, and mount namespaces */
    child_pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &child_cfg);

    if (child_pid < 0) {
        free(stack);
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "clone() failed: %s", strerror(errno));
        return -1;
    }

    /* Parent: close write end of pipe */
    close(pipe_fds[1]);

    /* Create container record */
    rec = calloc(1, sizeof(*rec));
    if (!rec) {
        close(pipe_fds[0]);
        kill(child_pid, SIGKILL);
        waitpid(child_pid, NULL, 0);
        free(stack);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return -1;
    }

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->host_pid = child_pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->nice_value = req->nice_value;
    rec->pipe_read_fd = pipe_fds[0];
    rec->stop_requested = 0;
    rec->exit_code = -1;
    rec->exit_signal = 0;
    rec->producer_active = 1;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log",
             LOG_DIR, req->container_id);

    /* For 'run' command, store the client fd to send exit status later */
    if (req->kind == CMD_RUN) {
        rec->run_client_fd = client_fd;
    } else {
        rec->run_client_fd = -1;
    }

    /* Add to container list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with kernel memory monitor */
    register_with_monitor(ctx->monitor_fd, rec->id, child_pid,
                          req->soft_limit_bytes, req->hard_limit_bytes);

    /* Start producer thread to read container output */
    parg = malloc(sizeof(*parg));
    if (parg) {
        parg->ctx = ctx;
        strncpy(parg->container_id, req->container_id,
                sizeof(parg->container_id) - 1);
        parg->pipe_fd = pipe_fds[0];

        if (pthread_create(&rec->producer_thread, NULL,
                           producer_thread_fn, parg) != 0) {
            fprintf(stderr, "[supervisor] Failed to create producer thread for %s\n",
                    req->container_id);
            free(parg);
            rec->producer_active = 0;
        }
    }

    /* Note: stack is intentionally not freed here —
     * it's needed while the child is alive.
     * In a production system we'd track it for cleanup. */

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "Container '%s' started, pid=%d", req->container_id, child_pid);

    fprintf(stderr, "[supervisor] Launched container '%s' pid=%d nice=%d "
            "soft=%luMiB hard=%luMiB\n",
            req->container_id, child_pid, req->nice_value,
            req->soft_limit_bytes >> 20, req->hard_limit_bytes >> 20);

    return 0;
}

/*
 * Handle the 'ps' command: build a table of all container metadata.
 */
static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    char buf[CONTROL_MESSAGE_LEN];
    int off = 0;

    off += snprintf(buf + off, sizeof(buf) - off,
                    "%-12s %-8s %-10s %-12s %-10s %-10s %-8s %-8s\n",
                    "CONTAINER", "PID", "STATE", "STARTED",
                    "SOFT(MiB)", "HARD(MiB)", "EXIT", "SIGNAL");
    off += snprintf(buf + off, sizeof(buf) - off,
                    "%-12s %-8s %-10s %-12s %-10s %-10s %-8s %-8s\n",
                    "----------", "------", "--------", "----------",
                    "--------", "--------", "------", "------");

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec && off < (int)sizeof(buf) - 200) {
        char time_buf[32];
        struct tm *tm_info = localtime(&rec->started_at);
        strftime(time_buf, sizeof(time_buf), "%H:%M:%S", tm_info);

        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-12s %-8d %-10s %-12s %-10lu %-10lu %-8d %-8d\n",
                        rec->id, rec->host_pid,
                        state_to_string(rec->state),
                        time_buf,
                        rec->soft_limit_bytes >> 20,
                        rec->hard_limit_bytes >> 20,
                        rec->exit_code,
                        rec->exit_signal);
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    strncpy(resp->message, buf, sizeof(resp->message) - 1);
}

/*
 * Handle the 'logs' command: read and return the container's log file.
 */
static void handle_logs(supervisor_ctx_t *ctx, const char *container_id,
                        control_response_t *resp)
{
    char log_path[PATH_MAX];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, container_id);

    int fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "No log file for container '%s'", container_id);
        return;
    }

    ssize_t n = read(fd, resp->message, sizeof(resp->message) - 1);
    if (n > 0) {
        resp->message[n] = '\0';
        resp->status = 0;
    } else {
        resp->status = 0;
        snprintf(resp->message, sizeof(resp->message), "(empty log)");
    }
    close(fd);
}

/*
 * Handle the 'stop' command: send SIGTERM, then escalate to SIGKILL.
 */
static void handle_stop(supervisor_ctx_t *ctx, const char *container_id,
                        control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container(ctx, container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' not found", container_id);
        return;
    }

    if (rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' is not running (state=%s)",
                 container_id, state_to_string(rec->state));
        return;
    }

    /* Set stop_requested BEFORE signaling — for attribution */
    rec->stop_requested = 1;
    pid_t pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Send SIGTERM first */
    fprintf(stderr, "[supervisor] Sending SIGTERM to container '%s' pid=%d\n",
            container_id, pid);
    kill(pid, SIGTERM);

    /* Wait briefly for graceful exit, then SIGKILL */
    usleep(500000); /* 500ms */

    pthread_mutex_lock(&ctx->metadata_lock);
    rec = find_container(ctx, container_id);
    if (rec && rec->state == CONTAINER_RUNNING) {
        fprintf(stderr, "[supervisor] Sending SIGKILL to container '%s' pid=%d\n",
                container_id, pid);
        kill(pid, SIGKILL);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "Stop signal sent to container '%s'", container_id);
}

/*
 * Reap exited children and update container metadata.
 * Called from SIGCHLD handler context.
 */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code = 128 + rec->exit_signal;

                    /* Attribution: distinguish stop vs hard-limit kill */
                    if (rec->stop_requested) {
                        rec->state = CONTAINER_STOPPED;
                    } else if (rec->exit_signal == SIGKILL) {
                        rec->state = CONTAINER_KILLED; /* hard_limit_killed */
                    } else {
                        rec->state = CONTAINER_KILLED;
                    }
                }

                fprintf(stderr, "[supervisor] Reaped container '%s' pid=%d "
                        "state=%s exit_code=%d signal=%d\n",
                        rec->id, pid, state_to_string(rec->state),
                        rec->exit_code, rec->exit_signal);

                /* Unregister from kernel monitor */
                unregister_from_monitor(ctx->monitor_fd, rec->id, pid);

                /* If this was a 'run' command, send exit status to client */
                if (rec->run_client_fd >= 0) {
                    control_response_t run_resp;
                    memset(&run_resp, 0, sizeof(run_resp));
                    run_resp.status = rec->exit_code;
                    snprintf(run_resp.message, sizeof(run_resp.message),
                             "Container '%s' exited: code=%d signal=%d state=%s",
                             rec->id, rec->exit_code, rec->exit_signal,
                             state_to_string(rec->state));
                    write(rec->run_client_fd, &run_resp, sizeof(run_resp));
                    close(rec->run_client_fd);
                    rec->run_client_fd = -1;
                }

                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* =====================================================
 * Signal Handlers
 * ===================================================== */

static void sigchld_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        reap_children(g_ctx);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx) {
        g_ctx->should_stop = 1;
    }
}

/* =====================================================
 * Supervisor Main Loop (Task 1 & 2)
 * ===================================================== */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;
    struct sigaction sa;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    ctx.should_stop = 0;
    strncpy(ctx.base_rootfs, rootfs, sizeof(ctx.base_rootfs) - 1);

    /* Set global pointer for signal handlers */
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 1) Open /dev/container_monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] Warning: Cannot open /dev/container_monitor: %s\n"
                "  (kernel module may not be loaded — memory monitoring disabled)\n",
                strerror(errno));
        /* Continue without monitor — non-fatal */
    } else {
        fprintf(stderr, "[supervisor] Kernel monitor connected (fd=%d)\n",
                ctx.monitor_fd);
    }

    /* 2) Create logs directory */
    mkdir(LOG_DIR, 0755);

    /* 3) Create the control socket (Unix domain socket) */
    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Control socket listening at %s\n", CONTROL_PATH);

    /* 4) Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigterm_handler;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 5) Spawn the logger (consumer) thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        fprintf(stderr, "[supervisor] Failed to create logger thread: %s\n",
                strerror(rc));
        goto cleanup;
    }

    fprintf(stderr, "[supervisor] Started for base-rootfs: %s\n", rootfs);
    fprintf(stderr, "[supervisor] PID=%d, ready for commands\n", getpid());

    /* 6) Supervisor event loop — accept control requests */
    while (!ctx.should_stop) {
        int client_fd;
        control_request_t req;
        control_response_t resp;
        struct timeval tv;
        fd_set readfds;

        /* Use select with timeout to check should_stop periodically */
        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        rc = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (rc == 0)
            continue; /* timeout, re-check should_stop */

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            perror("accept");
            continue;
        }

        /* Read control request */
        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != sizeof(req)) {
            fprintf(stderr, "[supervisor] Bad request (read %zd bytes)\n", n);
            close(client_fd);
            continue;
        }

        memset(&resp, 0, sizeof(resp));

        switch (req.kind) {
        case CMD_START:
            fprintf(stderr, "[supervisor] CMD_START id=%s rootfs=%s cmd=%s\n",
                    req.container_id, req.rootfs, req.command);
            launch_container(&ctx, &req, &resp, -1);
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            break;

        case CMD_RUN:
            fprintf(stderr, "[supervisor] CMD_RUN id=%s rootfs=%s cmd=%s\n",
                    req.container_id, req.rootfs, req.command);
            /* Don't close client_fd — run_client_fd will be set and
             * the response will be sent when the container exits */
            if (launch_container(&ctx, &req, &resp, client_fd) != 0) {
                /* Launch failed — send error and close */
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
            }
            /* On success, client_fd stays open until container exits */
            break;

        case CMD_PS:
            fprintf(stderr, "[supervisor] CMD_PS\n");
            handle_ps(&ctx, &resp);
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            break;

        case CMD_LOGS:
            fprintf(stderr, "[supervisor] CMD_LOGS id=%s\n", req.container_id);
            handle_logs(&ctx, req.container_id, &resp);
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            break;

        case CMD_STOP:
            fprintf(stderr, "[supervisor] CMD_STOP id=%s\n", req.container_id);
            handle_stop(&ctx, req.container_id, &resp);
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            break;

        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "Unknown command");
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            break;
        }
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            if (rec->state == CONTAINER_RUNNING) {
                rec->stop_requested = 1;
                fprintf(stderr, "[supervisor] Stopping container '%s' pid=%d\n",
                        rec->id, rec->host_pid);
                kill(rec->host_pid, SIGTERM);
            }
            rec = rec->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait for containers to exit */
    usleep(1000000); /* 1 second grace */

    /* Force-kill remaining */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            if (rec->state == CONTAINER_RUNNING) {
                fprintf(stderr, "[supervisor] Force-killing container '%s' pid=%d\n",
                        rec->id, rec->host_pid);
                kill(rec->host_pid, SIGKILL);
            }
            rec = rec->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Reap remaining children */
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    /* Shut down logging pipeline */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    fprintf(stderr, "[supervisor] Logger thread joined\n");

    /* Wait for producer threads */
    pthread_mutex_lock(&ctx.metadata_lock);
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            if (rec->producer_active) {
                pthread_t pt = rec->producer_thread;
                pthread_mutex_unlock(&ctx.metadata_lock);
                pthread_join(pt, NULL);
                pthread_mutex_lock(&ctx.metadata_lock);
            }
            rec = rec->next;
        }
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

cleanup:
    /* Free container records */
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            container_record_t *next = rec->next;
            if (rec->pipe_read_fd >= 0)
                close(rec->pipe_read_fd);
            if (rec->run_client_fd >= 0)
                close(rec->run_client_fd);
            free(rec);
            rec = next;
        }
    }

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    unlink(CONTROL_PATH);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[supervisor] Clean shutdown complete\n");
    g_ctx = NULL;
    return 0;
}

/* =====================================================
 * CLI Client (Task 2)
 * ===================================================== */

/*
 * Send a control request to the running supervisor over
 * a Unix domain socket. This is IPC Path B (control),
 * distinct from the logging pipes (Path A).
 */
static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(sock);
        return 1;
    }

    /* Send the request */
    n = write(sock, req, sizeof(*req));
    if (n != sizeof(*req)) {
        fprintf(stderr, "Failed to send request\n");
        close(sock);
        return 1;
    }

    /* Read the response */
    n = read(sock, &resp, sizeof(resp));
    if (n <= 0) {
        fprintf(stderr, "No response from supervisor\n");
        close(sock);
        return 1;
    }

    /* Print response */
    if (resp.message[0] != '\0') {
        printf("%s\n", resp.message);
    }

    close(sock);
    return (resp.status != 0) ? 1 : 0;
}

/*
 * Send a 'run' request: keep the connection open and wait
 * for the container to exit before returning.
 */
static int send_run_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t n;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n",
                CONTROL_PATH, strerror(errno));
        close(sock);
        return 1;
    }

    /* Send the request */
    n = write(sock, req, sizeof(*req));
    if (n != sizeof(*req)) {
        fprintf(stderr, "Failed to send request\n");
        close(sock);
        return 1;
    }

    /* Wait for exit status (blocks until container exits) */
    fprintf(stderr, "Waiting for container '%s' to exit...\n", req->container_id);
    n = read(sock, &resp, sizeof(resp));
    if (n <= 0) {
        fprintf(stderr, "Lost connection to supervisor\n");
        close(sock);
        return 1;
    }

    /* Print response */
    if (resp.message[0] != '\0') {
        printf("%s\n", resp.message);
    }

    close(sock);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_run_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
