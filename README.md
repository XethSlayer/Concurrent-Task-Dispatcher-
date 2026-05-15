# Concurrent Task Dispatcher in Rust

## Project Summary

This project is a Rust simulation of a concurrent task dispatcher. The program creates CPU and IO tasks, places them into queues, and sends them to a worker pool using different scheduling methods.

The project uses threads, queues, worker pools, shared state, synchronization, and performance metrics.

The two scheduling systems tested are FIFO scheduling and optimized scheduling with separate CPU and IO queues.

## How To Run

Build the project:

```bash
cargo build
```

Run the project:

```bash
cargo run
```

## Features

- 1000 randomly generated tasks
- CPU and IO task simulation
- 8 worker threads
- FIFO scheduler
- Optimized scheduler
- Monitor logging
- CSV output
- Performance metrics
- Clean shutdown

## Experiments

### Experiment A — Balanced Workload

This experiment uses 70% IO tasks and 30% CPU tasks. The optimized scheduler lowered average wait times by separating CPU and IO work.

### Experiment B — CPU Heavy Workload

This experiment uses 20% IO tasks and 80% CPU tasks. The optimized scheduler still lowered average wait times under heavier CPU stress.

## Metrics Collected

The program records total runtime, makespan, tasks completed, average wait time, average turnaround time, max wait time, average CPU usage, and average active workers.

## Monitor Output

The monitor thread records CPU usage, queue length, and active workers. The results are saved in `monitor_log.csv`.

## Design Overview

The main parts of the program are the task generator, task queues, worker threads, and monitor thread. The program uses `Arc`, `Mutex`, and atomic variables for synchronization.

## Tool Use Disclosure

Tools used were ChatGPT and Rust documentation.

One piece of advice I accepted was using `Arc<Mutex<_>>` for shared queues.

One thing I changed was the task duration setup. At first, FIFO and optimized scheduling behaved too similarly because all tasks had the same duration. I adjusted CPU tasks to take longer than IO tasks so the experiment results showed a clearer difference.
