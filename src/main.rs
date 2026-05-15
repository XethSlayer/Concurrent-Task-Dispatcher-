use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    collections::VecDeque,
    fs::File,
    io::Write,
    sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}},
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
enum TaskKind { CPU, IO }

#[derive(Debug, Clone)]
struct Task {
    id: usize,
    kind: TaskKind,
    duration: u64,
    cpu_cost: usize,
}

struct ResultData {
    id: usize,
    kind: TaskKind,
    wait: u128,
    turnaround: u128,
}

struct Sample {
    time: u128,
    cpu: usize,
    active: usize,
    queue: usize,
}

fn make_tasks(count: usize, io_percent: f64) -> Vec<Task> {
    let mut rng = StdRng::seed_from_u64(42);

    (0..count).map(|id| {
        let kind = if rng.gen_bool(io_percent) { TaskKind::IO } else { TaskKind::CPU };
        let cpu_cost = match kind { TaskKind::IO => 10, TaskKind::CPU => 35 };
        let duration = match kind { TaskKind::IO => 120, TaskKind::CPU => 300 };

        Task { id, kind, duration, cpu_cost }
    }).collect()
}

fn monitor(
    start: Instant,
    done: Arc<AtomicBool>,
    cpu: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    qlen: Arc<AtomicUsize>,
    samples: Arc<Mutex<Vec<Sample>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !done.load(Ordering::SeqCst) {
            samples.lock().unwrap().push(Sample {
                time: start.elapsed().as_millis(),
                cpu: cpu.load(Ordering::SeqCst),
                active: active.load(Ordering::SeqCst),
                queue: qlen.load(Ordering::SeqCst),
            });
            thread::sleep(Duration::from_millis(10));
        }
    })
}

fn run(name: &str, io_percent: f64, optimized: bool) {
    let count = 1000;
    let workers = 8;
    let cpu_cap = 100;

    let tasks = make_tasks(count, io_percent);
    let fifo_q = Arc::new(Mutex::new(VecDeque::from(tasks.clone())));
    let cpu_q = Arc::new(Mutex::new(VecDeque::from(
        tasks.iter().filter(|t| matches!(t.kind, TaskKind::CPU)).cloned().collect::<Vec<_>>()
    )));
    let io_q = Arc::new(Mutex::new(VecDeque::from(
        tasks.iter().filter(|t| matches!(t.kind, TaskKind::IO)).cloned().collect::<Vec<_>>()
    )));

    let results = Arc::new(Mutex::new(Vec::<ResultData>::new()));
    let cpu = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let qlen = Arc::new(AtomicUsize::new(count));
    let done = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));

    let start = Instant::now();
    let mon = monitor(start, done.clone(), cpu.clone(), active.clone(), qlen.clone(), samples.clone());

    let mut handles = vec![];

    for _ in 0..workers {
        let fifo_q = fifo_q.clone();
        let cpu_q = cpu_q.clone();
        let io_q = io_q.clone();
        let results = results.clone();
        let cpu = cpu.clone();
        let active = active.clone();
        let qlen = qlen.clone();

        handles.push(thread::spawn(move || loop {
            let task = if optimized {
                let mut chosen = None;

                if cpu.load(Ordering::SeqCst) < cpu_cap {
                    let mut cq = cpu_q.lock().unwrap();
                    if let Some(t) = cq.front() {
                        if cpu.load(Ordering::SeqCst) + t.cpu_cost <= cpu_cap {
                            chosen = cq.pop_front();
                        }
                    }
                }

                if chosen.is_none() {
                    chosen = io_q.lock().unwrap().pop_front();
                }

                if chosen.is_none() {
                    chosen = cpu_q.lock().unwrap().pop_front();
                }

                let remaining = cpu_q.lock().unwrap().len() + io_q.lock().unwrap().len();
                qlen.store(remaining, Ordering::SeqCst);
                chosen
            } else {
                let mut q = fifo_q.lock().unwrap();
                let chosen = q.pop_front();
                qlen.store(q.len(), Ordering::SeqCst);
                chosen
            };

            match task {
                Some(t) => {
                    active.fetch_add(1, Ordering::SeqCst);
                    cpu.fetch_add(t.cpu_cost, Ordering::SeqCst);

                    let task_start = Instant::now();
                    thread::sleep(Duration::from_millis(t.duration));

                    cpu.fetch_sub(t.cpu_cost, Ordering::SeqCst);
                    active.fetch_sub(1, Ordering::SeqCst);

                    results.lock().unwrap().push(ResultData {
                        id: t.id,
                        kind: t.kind,
                        wait: task_start.duration_since(start).as_millis(),
                        turnaround: Instant::now().duration_since(start).as_millis(),
                    });
                }
                None => break,
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    done.store(true, Ordering::SeqCst);
    mon.join().unwrap();

    print_results(name, count, workers, io_percent, start, results, samples);
}

fn print_results(
    name: &str,
    count: usize,
    workers: usize,
    io_percent: f64,
    start: Instant,
    results: Arc<Mutex<Vec<ResultData>>>,
    samples: Arc<Mutex<Vec<Sample>>>,
) {
    let r = results.lock().unwrap();
    let s = samples.lock().unwrap();

    let total = r.len();
    let runtime = start.elapsed().as_millis();
    let avg_wait = r.iter().map(|x| x.wait).sum::<u128>() as f64 / total as f64;
    let avg_turn = r.iter().map(|x| x.turnaround).sum::<u128>() as f64 / total as f64;
    let max_wait = r.iter().map(|x| x.wait).max().unwrap_or(0);
    let last_task_id = r.iter().map(|x| x.id).max().unwrap_or(0);
    let cpu_count = r.iter().filter(|x| matches!(x.kind, TaskKind::CPU)).count();
    let io_count = r.iter().filter(|x| matches!(x.kind, TaskKind::IO)).count();

    let avg_cpu = s.iter().map(|x| x.cpu as f64).sum::<f64>() / s.len() as f64;
    let avg_active = s.iter().map(|x| x.active as f64).sum::<f64>() / s.len() as f64;

    let mut file = File::create("monitor_log.csv").unwrap();
    writeln!(file, "time_ms,cpu_usage,active_workers,queue_len").unwrap();
    for x in s.iter() {
        writeln!(file, "{},{},{},{}", x.time, x.cpu, x.active, x.queue).unwrap();
    }

    println!("\n== {} ==", name);
    println!("{} tasks, {:.0}% IO / {:.0}% CPU, {} workers, cap 100%", count, io_percent * 100.0, (1.0 - io_percent) * 100.0, workers);
    println!("— results —");
    println!("total runtime          : {} ms", runtime);
    println!("makespan               : {} ms", runtime);
    println!("tasks completed        : {} (IO={}, CPU={})", total, io_count, cpu_count);
    println!("last task id completed : {}", last_task_id);
    println!("avg wait time          : {:.2} ms", avg_wait);
    println!("avg turnaround time    : {:.2} ms", avg_turn);
    println!("max wait time          : {} ms", max_wait);
    println!("avg CPU usage          : {:.2} %", avg_cpu);
    println!("avg workers active     : {:.2} / {}", avg_active, workers);
    println!("monitor samples        : {}", s.len());
    println!("monitor csv            : monitor_log.csv");
}

fn main() {
    println!("EXPERIMENT A: Balanced workload");
    run("FIFO simulation", 0.70, false);
    run("Optimized simulation", 0.70, true);

    println!("\nEXPERIMENT B: Stressed CPU-heavy workload");
    run("FIFO stressed simulation", 0.20, false);
    run("Optimized stressed simulation", 0.20, true);
}
