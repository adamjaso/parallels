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
#define WORKER_ARG_SIZE 1024
#define ENV_WORKER_ARG "WORKER_ARG"
#define ENV_WORKER_PID "WORKER_PID"
#define ENV_WORKER_INDEX "WORKER_INDEX"

void show_usage(char *msg) {
    dprintf(STDERR_FILENO,
            "%s"                                                                             \
            "Usage: parallels [-n NUM_PROCESSES] [-v] -- CMD [ARG ...]\n"                    \
            "  Note that each line read from STDIN must be less than or equal to %d bytes\n" \
            "\n"                                                                             \
            "  -n NUM_PROCESSES\n"                                                           \
            "     Indicates the integer number of worker processes\n"                        \
            "     Must be greater than or equal to %d and less than or equal to %d\n"        \
            "     Default is %d\n"                                                           \
            "  -v\n"                                                                         \
            "     Enable verbose logging of worker activity to STDERR\n"                     \
            "  CMD\n"                                                                        \
            "     Command each worker should exec against each line of input\n"              \
            "  ARG\n"                                                                        \
            "     Optional arguments to be passed along with CMD\n", msg, WORKER_ARG_SIZE, NPROC_MIN, NPROC_MAX, NPROC_DEFAULT);
}

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

void worker_wait(pid_t pid, int quiet);
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
    char worker_arg[WORKER_ARG_SIZE];
};

int env_init(struct env_t *env, int nenv, int worker_index, pid_t worker_pid, char *worker_arg);
void env_destroy(struct env_t *env);
void env_count(char **env, int *count);
void env_show(char **env);

// main

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

void worker_start(struct args_t *args) {
    // declare
    pid_t *pid;
    int *wqs;
    char *prefix = "main : ";
    struct reader_t reader;
    fd_set wfds, wfds2;
    struct timeval timeout;
    int i, ret, nfds, last_i;

    // assign
    FD_ZERO(&wfds);
    FD_ZERO(&wfds2);
    pid = calloc(args->nproc, sizeof (pid_t));
    wqs = calloc(args->nproc, sizeof (int));

    // start
    for (i = 0; i < args->nproc; i++) {
        int queue[2];
        ret = pipe(queue);
        if (ret == -1) {
            dprintf(STDERR_FILENO, "pipe %d\n", errno);
            break;
        }
        if (args->verbose) dprintf(STDERR_FILENO, "worker %d starting...\n", i);
        wqs[i] = queue[1];
        nfds = queue[1] + 1;
        FD_SET(queue[1], &wfds);            // watch write fd
        ret = fork();
        if (ret < 0) {
            dprintf(STDERR_FILENO, "failed to fork %d with error %d\n", i, errno);
            break;

        } else if (ret > 0) {
            // parent
            close(queue[0]);                // close read fd
            pid[i] = ret;

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
                if (ret < 0) dprintf(STDERR_FILENO, "select %d\n", errno);  // we have an error while waiting
                continue;
            }
            // worker available! find the worker...
            for (i = 0; i < args->nproc; i++, last_i = (last_i++) % args->nproc) {
                if (FD_ISSET(wqs[last_i], &wfds2)) {
                    // found worker! write line to it...
                    if (write(wqs[last_i], reader.line, reader.nread) == -1) {
                        dprintf(STDERR_FILENO, "write %d errno %d\n", wqs[last_i], errno);
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
            if (args->verbose) dprintf(STDERR_FILENO, "worker %d closing...\n", i);
            if (close(wqs[i]) == -1) {
                dprintf(STDERR_FILENO, "worker %d (%d) close error %d\n", i, pid[i], errno);
            }
        }

        // wait for workers to exit
        for (i = 0; i < args->nproc; i++) {
            if (args->verbose) dprintf(STDERR_FILENO, "worker %d (%d) wait...\n", i, pid[i]);
            worker_wait(pid[i], !args->verbose);
        }
    }

    free(pid);
    free(wqs);
}

void worker_child(struct args_t *args, int i) {
    struct reader_t reader;
    char *prefix = calloc(32, sizeof (char));
    pid_t child, worker_pid;

    worker_pid = getpid();
    sprintf(prefix, "worker %d (%d) :", i, getpid());

    if (args->verbose) dprintf(STDERR_FILENO, "worker %d started!\n", i);
    reader_init(&reader, prefix, stdin);
    while (!reader_read(&reader)) {
        child = fork();
        if (child < 0) {
            dprintf(STDERR_FILENO, "%s failed to process %s with error %d\n", prefix, reader.line, errno);
        } else if (child > 0) {
            worker_wait(child, 1);
        } else {
            struct env_t env;
            env_init(&env, args->nenv, i, worker_pid, reader.line);
            execve(args->cmd[0], args->cmd, env.env);
            env_destroy(&env);
        }
        reader_reset(&reader);
    }
    reader_reset(&reader);
    free(prefix);
    exit(EXIT_SUCCESS);
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
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--") == 0) {
            // we found cmd
            if (i + 1 >= argc) {
                show_usage("-- a command must follow\n");
                return -1;
            }
            int j;
            for (j = 0; i < argc; j++) args->cmd[j] = argv[++i];
            args->cmd[j - 1] = NULL;
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
        } else if (strcmp(argv[i], "-h") == 0) {
            show_usage("");
            exit(EXIT_SUCCESS);
        }
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
                dprintf(STDERR_FILENO, "getline %d", errno);
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
    if (strlen(worker_arg) + strlen(ENV_WORKER_ARG) + 1 > WORKER_ARG_SIZE) {
        dprintf(STDERR_FILENO, "%s is too large\n", ENV_WORKER_ARG);
        return -1;
    }

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

void worker_wait(pid_t pid, int quiet) {
    // parent
    pid_t wstatus;
    waitpid(pid, &wstatus, 0);
    if (quiet) return;
    if (WIFEXITED(wstatus)) {
        dprintf(STDERR_FILENO, "worker %d exited with status %d\n", pid, WEXITSTATUS(wstatus));
        //exit(EXIT_SUCCESS);
    } else if (WIFSIGNALED(wstatus)) {
        dprintf(STDERR_FILENO, "worker %d killed by signal %d%s\n", pid, WTERMSIG(wstatus), WCOREDUMP(wstatus) ? " (dumped core)" : "");
        //exit(EXIT_FAILURE);
    } else if (WIFSTOPPED(wstatus)) {
        dprintf(STDERR_FILENO, "worker %d stopped by signal %d\n", pid, WSTOPSIG(wstatus));
    } else if (WIFCONTINUED(wstatus)) {
        dprintf(STDERR_FILENO, "worker %d continued\n", pid);
    } else {
        dprintf(STDERR_FILENO, "worker %d exited with unknown status %d\n", pid, wstatus);
        //exit(EXIT_FAILURE);
    }
}
