/*
 * Copyright 2019 Adam Jaso
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <sys/eventfd.h>
#include <sys/sysinfo.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

// args

#define EVENT_READY 1
#define EVENT_INUSE 0
#define NPROC_DEFAULT 8
#define NPROC_MIN 2
#define NPROC_MAX (sysconf(_SC_OPEN_MAX) / 2)
#define ENV_WORKER_ARG "WORKER_ARG"
#define ENV_WORKER_PID "WORKER_PID"
#define ENV_WORKER_INDEX "WORKER_INDEX"
#define ENV_WORKER_EVENTFD "WORKER_EVENTFD"
#define WORKER_ARG_BUFSIZE 1024
#define WORKER_ARG_SIZE WORKER_ARG_BUFSIZE - strlen(ENV_WORKER_ARG) - 1 - 1
#define MAIN_PREFIX "main : "

struct args_t {
    int nproc;
    int nenv;
    int verbose;
    char **cmd;
};

void args_init(struct args_t *args, int argc);
void args_destroy(struct args_t *args);
void args_show(struct args_t *args);
int args_parse(int argc, char **argv, struct args_t *args);

// worker

struct worker_t {
    int index;
    char prefix[32];
    pid_t pid;
    int pipe_fd;
    int event_fd;
    unsigned long long event_data;
};

int worker_init(struct worker_t *worker, int index);
void worker_start(struct args_t *args);
void worker_run(struct args_t *args, struct worker_t *worker, int *queue);
int worker_fork(struct args_t *args, struct worker_t *worker, int index);
void worker_wait(pid_t pid, char *prefix, int verbose);
void main_readlines(struct args_t *args, struct worker_t *worker);

// reader

struct reader_t {
    FILE *file;
    char *line;
    char *prefix;
    size_t linesize;
    ssize_t nread;
    int err;
    int done;
};

void reader_init(struct reader_t *reader, char *prefix, FILE *file);
int reader_read(struct reader_t *reader);
void reader_reset(struct reader_t *reader);
void reader_log(struct reader_t *reader);
void reader_show(struct reader_t *reader);

// env

struct env_t {
    char **env;
    char worker_index[32];
    char worker_pid[32];
    char worker_arg[WORKER_ARG_BUFSIZE];
};

int env_init(struct env_t *env, int nenv, int worker_index, pid_t worker_pid);
void env_destroy(struct env_t *env);
void env_count(char **env, int *count);
void env_show(char **env);

// main

void show_usage(char *msg);

int main(int argc, char **argv) {
    struct args_t args;
    args_init(&args, argc);
    if (args_parse(argc, argv, &args) == 0) {
        if (args.verbose) args_show(&args);
        worker_start(&args);
    }
    args_destroy(&args);
    return 0;
}

void show_usage(char *msg) {
    // buffer_size - worker_arg_name - equals_char - NUL
    dprintf(STDERR_FILENO,
            "%s"                                                                                \
            "Usage: parallels [-n NUM_PROCESSES] [-v] -- CMD [ARG ...]\n"                       \
            "\n"                                                                                \
            "Job runner that reads lines from STDIN and passes each line to parallel worker\n"  \
            "processes that process the lines as needed.\n"                                     \
            "\n"                                                                                \
            "When a worker starts, it forks and executes the command (CMD + ARG). Whenever it\n"\
            "finishes processing a line from STDIN, it must write an 8 byte number to the\n"    \
            "event file descriptor stored in WORKER_EVENTFD\n"                                  \
            "(i.e. echo -ne \"\\x1\\x0\\x0\\x0\\x0\\x0\\x0\\x0\" >&${WORKER_EVENTFD}).\n"       \
            "A full list of worker environment variables is listed below.\n"                    \
            "\n"                                                                                \
            "Note that each line read from STDIN must be less than or equal to %ld bytes\n"     \
            "\n"                                                                                \
            "The worker invokes the user-defined script with the following environment\n"       \
            "variables\n"                                                                       \
            "  - inherits the master's environment variables\n"                                 \
            "  - WORKER_EVENTFD an eventfd file descriptor used to signal when a process is\n"  \
            "                   ready to process another line\n"                                \
            "  - WORKER_INDEX   the index of the worker running the job\n"                      \
            "  - WORKER_PID     the pid of the worker running the job\n"                        \
            "\n"                                                                                \
            "Arguments:\n"                                                                      \
            "  -n NUM_PROCESSES\n"                                                              \
            "     Indicates the integer number of worker processes\n"                           \
            "     Must be greater than or equal to %d and less than or equal to %ld\n"          \
            "     Default is %d\n"                                                              \
            "  -v\n"                                                                            \
            "     Enable verbose logging of worker activity to STDERR\n"                        \
            "  CMD\n"                                                                           \
            "     Command to start your worker process\n"                                       \
            "  ARG\n"                                                                           \
            "     Optional arguments to be passed along with CMD\n"                             \
            "\n"                                                                                \
            "Examples:\n"                                                                       \
            "  $ cat urls.txt | parallels -n 16 -- \\\n"                                        \
            "      /bin/sh -c 'curl $WORKER_ARG > $(basename $WORKER_ARG)'\n"                   \
            "\n"                                                                                \
            "  Each line in urls.txt is a URL to download, and the worker will download up\n"   \
            "  to 16 URLs in parallel.\n"
            ,
            msg, WORKER_ARG_SIZE, NPROC_MIN, NPROC_MAX, get_nprocs());
}

// worker

void worker_start(struct args_t *args) {
    int i, ret;
    struct worker_t *workers;

    workers = calloc(args->nproc, sizeof (struct worker_t));

    for (i = 0; i < args->nproc; i++) {
        ret = worker_init(&workers[i], i);
        if (ret < 0) break;
    }

    if (ret >= 0) {
        for (i = 0; i < args->nproc; i++) {
            ret = worker_fork(args, workers, i);
            if (ret < 0) break;
        }
    }

    if (ret >= 0) {
        // read stdin till EOF
        main_readlines(args, workers);

        // close write fds
        for (i = 0; i < args->nproc; i++) {
            if (args->verbose)                  dprintf(STDERR_FILENO, "%s worker %d close...\n", MAIN_PREFIX, i);
            if (close(workers[i].pipe_fd) < 0)  dprintf(STDERR_FILENO, "%s worker %d close pipefd error %d\n", MAIN_PREFIX, i, errno);
            if (close(workers[i].event_fd) < 0) dprintf(STDERR_FILENO, "%s worker %d close eventfd error %d\n", MAIN_PREFIX, i, errno);
        }

        // wait for workers to exit
        for (i = 0; i < args->nproc; i++) {
            if (args->verbose) dprintf(STDERR_FILENO, "%s worker %d (%d) wait...\n", MAIN_PREFIX, i, workers[i].pid);
            worker_wait(workers[i].pid, MAIN_PREFIX, args->verbose);
        }
    }

    free(workers);
}

int worker_init(struct worker_t *worker, int index) {
    sprintf(worker->prefix, "worker %d :", index);
    worker->index = index;
    worker->event_fd = eventfd(EVENT_INUSE, 0);
    if (worker->event_fd < 0) dprintf(STDERR_FILENO, "%s starting worker %d failed with eventfd error %d\n", MAIN_PREFIX, index, errno);
    return worker->event_fd;
}

int worker_fork(struct args_t *args, struct worker_t *workers, int index) {
    struct worker_t *worker;
    int queue[2];
    int ret, i;

    worker = &workers[index];
    if (args->verbose) dprintf(STDERR_FILENO, "%s starting worker %d...\n", MAIN_PREFIX, worker->index);
    ret = pipe(queue);                  // setup worker pipe
    if (ret < 0) {
        dprintf(STDERR_FILENO, "%s starting worker %d failed with pipe error %d\n", MAIN_PREFIX, worker->index, errno);
        return ret;
    }
    ret = fork();                       // start worker
    if (ret < 0) {
        dprintf(STDERR_FILENO, "%s starting worker %d failed with fork error %d\n", MAIN_PREFIX, worker->index, errno);
        return ret;

    } else if (ret > 0) {
        // parent
        if (args->verbose) dprintf(STDERR_FILENO, "%s worker %d forked with pid %d\n", MAIN_PREFIX, worker->index, ret);
        close(queue[0]);                // close read fd
        worker->pid = ret;
        worker->pipe_fd = queue[1];            // save write fd
        return ret;
    }
    // child
    // close all file descriptors that we don't need
    for (i = 0; i < args->nproc; i++) {
        if (i != index) close(workers[i].event_fd); // leave our event fd intact
        if (i < index) close(workers[i].pipe_fd);   // leave our pipe fd intact
    }
    worker_run(args, worker, queue);
    return 0;
}

int worker_to_main(struct worker_t *worker, unsigned long long event_data) {
    int ret;
    worker->event_data = event_data;
    ret = write(worker->event_fd, &worker->event_data, sizeof(worker->event_data));
    if (ret < 0) dprintf(STDERR_FILENO, "%s failed on write eventfd %d\n", worker->prefix, errno);
    return ret;
}

int main_to_worker(struct worker_t *worker) {
    int ret;
    ret = read(worker->event_fd, &worker->event_data, sizeof (worker->event_data));
    if (ret < 0) dprintf(STDERR_FILENO, "%s failed on read eventfd %d for worker %d\n", MAIN_PREFIX, errno, worker->index);
    return ret;
}

void worker_run(struct args_t *args, struct worker_t *worker, int *queue) {
    struct env_t env;
    int ret;
    int exit_status;

    worker->pid = getpid();
    worker->pipe_fd = STDIN_FILENO;

    close(worker->pipe_fd);            // close stdin fd
    dup2(queue[0], worker->pipe_fd);   // dup read fd to stdin fd
    close(queue[1]);                // close write fd
    close(queue[0]);                // close read fd
    if (worker_to_main(worker, EVENT_READY) < 0) exit(EXIT_FAILURE);
    env_init(&env, args->nenv, worker->index, worker->pid);
    sprintf(env.worker_arg, "%s=%d", ENV_WORKER_EVENTFD, worker->event_fd);
    ret = execvpe(args->cmd[0], args->cmd, env.env);
    if (ret < 0) {
        dprintf(STDERR_FILENO, "%s failed to exec %s with error %d\n", worker->prefix, args->cmd[0], errno);
        exit_status = 1;
    } else {
        exit_status = 0;
    }
    env_destroy(&env);
    exit(exit_status);
}

void worker_wait(pid_t pid, char *prefix, int verbose) {
    // parent
    pid_t wstatus;
    waitpid(pid, &wstatus, 0);
    if (WIFEXITED(wstatus)) {
        if (verbose) dprintf(STDERR_FILENO, "%s pid %d exited with status %d\n", prefix, pid, WEXITSTATUS(wstatus));
    } else if (WIFSIGNALED(wstatus)) {
        if (WCOREDUMP(wstatus)) dprintf(STDERR_FILENO, "%s pid %d killed by signal %d (dumped core)\n", prefix, pid, WTERMSIG(wstatus));
        else if (verbose)       dprintf(STDERR_FILENO, "%s pid %d killed by signal %d\n", prefix, pid, WTERMSIG(wstatus));
    } else if (WIFSTOPPED(wstatus)) {
        if (verbose) dprintf(STDERR_FILENO, "%s pid %d stopped by signal %d\n", prefix, pid, WSTOPSIG(wstatus));
    } else if (WIFCONTINUED(wstatus)) {
        if (verbose) dprintf(STDERR_FILENO, "%s pid %d continued\n", prefix, pid);
    } else {
        dprintf(STDERR_FILENO, "%s pid %d exited with unknown status %d\n", prefix, pid, wstatus);
    }
}

void main_readlines(struct args_t *args, struct worker_t *workers) {
    int i, nfds, ret;
    fd_set wfds, wfds2;
    struct reader_t reader;

    FD_ZERO(&wfds);
    FD_ZERO(&wfds2);
    for (i = 0; i < args->nproc; i++) {
        nfds = workers[i].event_fd + 1;
        FD_SET(workers[i].event_fd, &wfds);          // watch write fd
    }

    reader_init(&reader, MAIN_PREFIX, stdin);
    while (!reader_read(&reader)) {                     // read line by line
        wfds2 = wfds;                                   // reset watched fds
        ret = select(nfds, &wfds2, NULL, NULL, NULL);   // wait for available worker
        if (ret <= 0) {                                 // check for error or timeout
            if (ret < 0) dprintf(STDERR_FILENO, "%s select got error %d\n", MAIN_PREFIX, errno);  // we have an error while waiting
            continue;
        }
        // worker available! find the worker...
        for (i = 0; i < args->nproc; i++) {
            if (FD_ISSET(workers[i].event_fd, &wfds2)) {
                // found worker! write line to it...
                // clear the status and prepare to write.
                if (main_to_worker(&workers[i]) < 0) {
                    break;
                }
                if (args->verbose) {
                    dprintf(STDERR_FILENO, "%s worker %d write...\n", MAIN_PREFIX, i);
                }
                // write line to worker
                if (write(workers[i].pipe_fd, reader.line, reader.nread) < 0) {
                    dprintf(STDERR_FILENO, "%s worker %d write error %d\n", MAIN_PREFIX, i, errno);
                }
                break;
            }
        }
        reader_reset(&reader);
    }
    reader_reset(&reader);
}

// args

void args_init(struct args_t *args, int argc) {
    memset(args, 0, sizeof (struct args_t));
    args->cmd = calloc(argc - 1, sizeof (char *));
    args->nproc = NPROC_DEFAULT;
    env_count(__environ, &args->nenv);
    args->verbose = 0;
}

void args_destroy(struct args_t *args) {
    free(args->cmd);
}

void args_show(struct args_t *args) {
    dprintf(STDERR_FILENO, "args.nproc  : %d\n", args->nproc);
    dprintf(STDERR_FILENO, "args.nenv   : %d\n", args->nenv);
    char **c;
    for (c = args->cmd; *c != NULL; c++) {
        dprintf(STDERR_FILENO, "args.cmd    : %s\n", *c);
    }
}

int args_parse(int argc, char **argv, struct args_t *args) {
    int ncmd;

    ncmd = 0;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--") == 0) {
            // we found cmd
            if (i + 1 >= argc) {
                show_usage("-- a command must follow\n");
                return -1;
            }
            for (ncmd = 0; i < argc; ncmd++) args->cmd[ncmd] = argv[++i];
            args->cmd[ncmd - 1] = NULL;
            break;

        } else if (strcmp(argv[i], "-n") == 0) {
            // we found nproc
            if (i + 1 >= argc) {
                show_usage("-n requires a value\n");
                return -1;
            }
            args->nproc = atoi(argv[++i]);
            if (args->nproc < NPROC_MIN || args->nproc > NPROC_MAX) {
                show_usage("-n is invalid\n");
                return -1;
            }

        } else if (strcmp(argv[i], "-v") == 0) {
            // we found verbose
            args->verbose = 1;

        } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            // we found help
            show_usage("");
            exit(EXIT_SUCCESS);
        }
    }
    if (ncmd < 2) {
        show_usage("CMD is required\n");
        return -1;
    }
    return 0;
}

// reader

void reader_init(struct reader_t *reader, char *prefix, FILE *file) {
    reader->prefix = prefix;
    reader->file = file;
    reader->err = 0;
    reader->done = 0;
}

int reader_read(struct reader_t *reader) {
    if (!reader->done) {
        reader->line = NULL;
        reader->linesize = 0;

        reader->nread = getline(&reader->line, &reader->linesize, reader->file);
        if (reader->nread == -1) {
            reader->done = 1;
            if (errno != 0) {
                reader->err = errno;
                dprintf(STDERR_FILENO, "%s getline %d\n", reader->prefix, errno);
            }
        }
    }
    return reader->done;
}

void reader_reset(struct reader_t *reader) {
    free(reader->line);
}

void reader_log(struct reader_t *reader) {
    if (reader->err) {
        dprintf(STDERR_FILENO, "%s reader error %d\n", reader->prefix, reader->err);
    } else {
        printf("%s%s", reader->prefix, reader->line);
    }
}

void reader_show(struct reader_t *reader) {
    for (int i = 1; !reader_read(reader); i++) {
        reader_log(reader);
    }
    if (reader->err) {
        reader_log(reader);
    }
}

// env

int env_init(struct env_t *env, int nenv, int worker_index, pid_t worker_pid) {
    char **e;
    int i;

    env->env = (char **) calloc(nenv + 3 + 1, sizeof (char *));
    for (i = 0, e = __environ; *e != NULL; e++) {
        env->env[i++] = *e;
    }
    sprintf(env->worker_pid, "%s=%d", ENV_WORKER_PID, worker_pid);
    sprintf(env->worker_index, "%s=%d", ENV_WORKER_INDEX, worker_index);
    env->env[i++] = env->worker_arg;
    env->env[i++] = env->worker_pid;
    env->env[i++] = env->worker_index;
    env->env[i++] = NULL;
    return 0;
}

void env_destroy(struct env_t *env) {
    free(env->env);
}

void env_show(char **env) {
    for (char **c = env; *c != NULL; c++) {
        dprintf(STDERR_FILENO, "env : %s\n", *c);
    }
}

void env_count(char **env, int *count) {
    char **e;
    for (e = env, *count = 0; *e != NULL; e++, (*count)++);
}
