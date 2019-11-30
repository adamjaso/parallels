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
#include <fcntl.h>
#include <unistd.h>

// args

#define NPROC_DEFAULT 8
#define NPROC_MIN 2
#define NPROC_MAX 64
#define ENV_WORKER_ARG "WORKER_ARG"
#define ENV_WORKER_PID "WORKER_PID"
#define ENV_WORKER_INDEX "WORKER_INDEX"
#define WORKER_ARG_BUFSIZE 1024
#define WORKER_ARG_SIZE WORKER_ARG_BUFSIZE - strlen(ENV_WORKER_ARG) - 1 - 1

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

void worker_wait(pid_t pid, char *prefix, int verbose);
void worker_start(struct args_t *args);
void worker_child(struct args_t *args, int index);

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

int env_init(struct env_t *env, int nenv, int worker_index, pid_t worker_pid, char *worker_arg);
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
            "Job runner that reads lines from STDIN and passes them to parallel worker\n"       \
            "processes that invoke a user-defined script against each line.\n"                  \
            "\n"                                                                                \
            "When a worker receives a job, it forks and executes the command (CMD + ARG), \n"   \
            "passing the job as an environment variable (WORKER_ARG). A full list of worker\n"  \
            "environment variables is listed below.\n"                                          \
            "\n"                                                                                \
            "Note that each line read from STDIN must be less than or equal to %ld bytes\n"     \
            "\n"                                                                                \
            "The worker invokes the user-defined script with the following environment\n"       \
            "variables\n"                                                                       \
            "  - all existing environment variables\n"                                          \
            "  - WORKER_ARG    the line from STDIN\n"                                           \
            "  - WORKER_INDEX  the index of the worker running the job\n"                       \
            "  - WORKER_PID    the pid of the worker running the job\n"                         \
            "\n"                                                                                \
            "Arguments:\n"                                                                      \
            "  -n NUM_PROCESSES\n"                                                              \
            "     Indicates the integer number of worker processes\n"                           \
            "     Must be greater than or equal to %d and less than or equal to %d\n"           \
            "     Default is %d\n"                                                              \
            "  -v\n"                                                                            \
            "     Enable verbose logging of worker activity to STDERR\n"                        \
            "  CMD\n"                                                                           \
            "     Command each worker should exec against each line of STDIN\n"                 \
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
            msg, WORKER_ARG_SIZE, NPROC_MIN, NPROC_MAX, NPROC_DEFAULT);
}

// worker

void worker_start(struct args_t *args) {
    pid_t *worker_pids;
    int *worker_fds;
    char *prefix = "main : ";
    struct reader_t reader;
    fd_set wfds, wfds2;
    int i, ret, nfds, last_i;

    FD_ZERO(&wfds);
    FD_ZERO(&wfds2);
    worker_pids = calloc(args->nproc, sizeof (pid_t));
    worker_fds = calloc(args->nproc, sizeof (int));

    for (i = 0; i < args->nproc; i++) {
        int queue[2];
        ret = pipe(queue);
        if (args->verbose) dprintf(STDERR_FILENO, "%s starting worker %d...\n", prefix, i);
        if (ret == -1) {
            dprintf(STDERR_FILENO, "%s starting worker %d failed with pipe error %d\n", prefix, i, errno);
            break;
        }
        worker_fds[i] = queue[1];
        nfds = queue[1] + 1;
        FD_SET(queue[1], &wfds);            // watch write fd
        ret = fork();
        if (ret < 0) {
            dprintf(STDERR_FILENO, "%s starting worker %d failed with fork error %d\n", prefix, i, errno);
            break;

        } else if (ret > 0) {
            // parent
            if (args->verbose) dprintf(STDERR_FILENO, "%s worker %d forked with pid %d\n", prefix, i, ret);
            close(queue[0]);                // close read fd
            worker_pids[i] = ret;

        } else {
            // child
            close(STDIN_FILENO);            // close stdin fd
            dup2(queue[0], STDIN_FILENO);   // dup read fd to stdin fd
            close(queue[1]);                // close write fd
            close(queue[0]);                // close read fd
            worker_child(args, i);
        }
    }

    if (ret >= 0) {
        // read stdin till EOF
        reader_init(&reader, prefix, stdin);
        last_i = 0;
        while (!reader_read(&reader)) {                     // read line by line
            wfds2 = wfds;                                   // reset watched fds
            ret = select(nfds, NULL, &wfds2, NULL, NULL);   // wait for available worker
            if (ret <= 0) {                                 // check for error or timeout
                if (ret < 0) dprintf(STDERR_FILENO, "%s select got error %d\n", prefix, errno);  // we have an error while waiting
                continue;
            }
            // worker available! find the worker...
            for (i = 0; i < args->nproc; i++, last_i = (last_i++) % args->nproc) {
                if (FD_ISSET(worker_fds[last_i], &wfds2)) {
                    // found worker! write line to it...
                    if (args->verbose) dprintf(STDERR_FILENO, "%s worker %d write...\n", prefix, last_i);
                    if (write(worker_fds[last_i], reader.line, reader.nread) == -1) {
                        dprintf(STDERR_FILENO, "%s worker %d write error %d\n", prefix, last_i, errno);
                    }
                    last_i ++;
                    break;
                }
            }
            reader_reset(&reader);
        }
        reader_reset(&reader);

        // close write fds
        for (i = 0; i < args->nproc; i++) {
            if (args->verbose)       dprintf(STDERR_FILENO, "%s worker %d close...\n", prefix, i);
            if (close(worker_fds[i]) == -1) dprintf(STDERR_FILENO, "%s worker %d close error %d\n", prefix, i, errno);
        }

        // wait for workers to exit
        for (i = 0; i < args->nproc; i++) {
            if (args->verbose) dprintf(STDERR_FILENO, "%s worker %d (%d) wait...\n", prefix, i, worker_pids[i]);
            worker_wait(worker_pids[i], prefix, args->verbose);
        }
    }

    free(worker_pids);
    free(worker_fds);
}

void worker_child(struct args_t *args, int i) {
    struct reader_t reader;
    char *prefix = calloc(32, sizeof (char));
    pid_t child, worker_pid;
    int ret, exit_status;

    exit_status = 0;
    worker_pid = getpid();
    sprintf(prefix, "worker %d :", i);

    if (args->verbose) dprintf(STDERR_FILENO, "%s started!\n", prefix);
    reader_init(&reader, prefix, stdin);
    while (!reader_read(&reader)) {
        struct env_t env;
        if (strlen(reader.line) > WORKER_ARG_SIZE) {
            dprintf(STDERR_FILENO, "%s %s is too large (%ld bytes)\n", prefix, ENV_WORKER_ARG, strlen(reader.line));
            continue;
        }
        env_init(&env, args->nenv, i, worker_pid, reader.line);
        child = fork();
        if (child < 0) {
            dprintf(STDERR_FILENO, "%s fork error %d\n", prefix, errno);
        } else if (child > 0) {
            worker_wait(child, prefix, args->verbose);
        } else {
            ret = execve(args->cmd[0], args->cmd, env.env);
            if (ret < 0) {
                dprintf(STDERR_FILENO, "%s failed to exec %s with error %d\n", prefix, args->cmd[0], errno);
                exit_status = 1;
            } else {
                exit_status = 0;
            }
            env_destroy(&env);
            exit(exit_status);
        }
        reader_reset(&reader);
    }
    reader_reset(&reader);
    free(prefix);
    exit(exit_status);
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
    dprintf(STDERR_FILENO, "args.nproc : %d\n", args->nproc);
    dprintf(STDERR_FILENO, "args.nenv  : %d\n", args->nenv);
    char **c;
    for (c = args->cmd; *c != NULL; c++) {
        dprintf(STDERR_FILENO, "args.cmd   : %s\n", *c);
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

int env_init(struct env_t *env, int nenv, int worker_index, pid_t worker_pid, char *worker_arg) {
    char **e;
    int i;

    env->env = (char **) calloc(nenv + 3 + 1, sizeof (char *));
    for (i = 0, e = __environ; *e != NULL; e++) {
        env->env[i++] = *e;
    }
    sprintf(env->worker_arg, "%s=%s", ENV_WORKER_ARG, worker_arg);
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

// worker

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
