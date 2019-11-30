# parallels

This project is a simple job runner that takes lines from `stdin` and passes
them to workers for processing.

## Build

```
make
```

Binary `./parallels` is output in the current directory.

## Usage

```
./parallels -n 8 -- /bin/bash -c 'echo -n "worker $WORKER_INDEX ($$) $WORKER_ARG"'
```

The variables `WORKER_INDEX` and `WORKER_ARG` contain the index of the worker
that started the script and the line (read from the master process's `stdin`)
passed to it, respectively.

## How does it work?

This program starts `n` workers and reads `stdin` line by line, sending each line
to one of the worker processes which `execve`s a user specified script passing
the line as an `environ` variable to the script.

## License

MIT
