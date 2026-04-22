/// End-to-end load test binary for LRANGE and XRANGE.
///
/// Env vars:
///   REDIS_ADDR              — server address (default: 127.0.0.1:6379)
///   LOAD_TEST_DURATION_SECS — measurement window per scenario (default: 10)
///   LOAD_TEST_OUTPUT        — CSV output file path (default: load_test_results.csv)
///
/// Usage:
///   1. Start the server:   cargo run
///   2. Run the load test:  cargo run --bin load_test
use std::{
    fs::{File, OpenOptions},
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::sleep,
};

// ── config ─────────────────────────────────────────────────────────────────

fn redis_addr() -> String {
    std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:6379".into())
}

fn duration_secs() -> u64 {
    std::env::var("LOAD_TEST_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

fn output_path() -> String {
    std::env::var("LOAD_TEST_OUTPUT").unwrap_or_else(|_| "load_test_results.csv".into())
}

// ── RESP helpers ───────────────────────────────────────────────────────────

/// Build a RESP array command frame.
fn resp_cmd(parts: &[&str]) -> String {
    let mut s = format!("*{}\r\n", parts.len());
    for p in parts {
        s.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    s
}

/// Skip (consume) exactly one complete RESP response from `reader`.
async fn skip_response<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> std::io::Result<()> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let prefix = line.chars().next().unwrap_or('+');
    match prefix {
        '+' | '-' | ':' => {} // single-line; done
        '$' => {
            // bulk string: read declared length, then the \r\n-terminated content
            let len: i64 = line[1..].trim().parse().unwrap_or(-1);
            if len >= 0 {
                let mut buf = vec![0u8; len as usize + 2]; // +2 for \r\n
                reader.read_exact(&mut buf).await?;
            }
        }
        '*' => {
            // array: recurse for each element
            let count: i64 = line[1..].trim().parse().unwrap_or(0);
            for _ in 0..count {
                Box::pin(skip_response(reader)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

// ── seeding ────────────────────────────────────────────────────────────────

/// Populate a Redis list key with `size` elements using batched RPUSH.
async fn seed_list(addr: &str, key: &str, size: usize) -> std::io::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    const BATCH: usize = 500;
    let mut seeded = 0usize;
    while seeded < size {
        let batch_end = (seeded + BATCH).min(size);
        let elements: Vec<String> = (seeded..batch_end).map(|i| format!("v{i}")).collect();

        let mut parts = vec!["RPUSH", key];
        let refs: Vec<&str> = elements.iter().map(|s| s.as_str()).collect();
        parts.extend_from_slice(&refs);
        wr.write_all(resp_cmd(&parts).as_bytes()).await?;
        skip_response(&mut reader).await?;
        seeded = batch_end;
    }
    Ok(())
}

/// Populate a Redis stream key with `size` entries using pipelined XADD.
async fn seed_stream(addr: &str, key: &str, size: usize) -> std::io::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);

    const PIPELINE: usize = 200;
    let mut sent = 0usize;
    while sent < size {
        let batch_end = (sent + PIPELINE).min(size);
        // Pipeline PIPELINE XADD commands
        let mut pipeline = String::new();
        let ids: Vec<String> = (sent..batch_end)
            .map(|i| format!("{}-0", i + 1))
            .collect();
        let vals: Vec<String> = (sent..batch_end).map(|i| format!("{i}")).collect();
        for (id, val) in ids.iter().zip(vals.iter()) {
            pipeline.push_str(&resp_cmd(&["XADD", key, id, "v", val]));
        }
        wr.write_all(pipeline.as_bytes()).await?;
        for _ in sent..batch_end {
            skip_response(&mut reader).await?;
        }
        sent = batch_end;
    }
    Ok(())
}

// ── delete a key ───────────────────────────────────────────────────────────

/// DEL a key so re-seeding is clean across scenarios.
async fn del_key(addr: &str, key: &str) -> std::io::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    wr.write_all(resp_cmd(&["DEL", key]).as_bytes()).await?;
    skip_response(&mut reader).await?;
    let _ = reader; // keep alive until done
    Ok(())
}

// ── single client measurement task ─────────────────────────────────────────

/// One client loop: connect once, send `cmd_frame` repeatedly until `stop`
/// is set, record each round-trip latency in nanoseconds.
async fn run_client(
    addr: String,
    cmd_frame: String,
    stop: Arc<AtomicBool>,
) -> std::io::Result<Vec<u64>> {
    let stream = TcpStream::connect(&addr).await?;
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut latencies = Vec::with_capacity(4096);

    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        wr.write_all(cmd_frame.as_bytes()).await?;
        skip_response(&mut reader).await?;
        latencies.push(t0.elapsed().as_nanos() as u64);
    }

    Ok(latencies)
}

// ── statistics ─────────────────────────────────────────────────────────────

struct Stats {
    total_ops: u64,
    elapsed_secs: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
}

fn compute_stats(mut latencies_ns: Vec<u64>, elapsed: Duration) -> Stats {
    let total_ops = latencies_ns.len() as u64;
    latencies_ns.sort_unstable();

    let percentile = |p: f64| -> f64 {
        if latencies_ns.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * (latencies_ns.len() - 1) as f64) as usize;
        latencies_ns[idx] as f64 / 1_000.0 // ns → µs
    };

    Stats {
        total_ops,
        elapsed_secs: elapsed.as_secs_f64(),
        p50_us: percentile(50.0),
        p95_us: percentile(95.0),
        p99_us: percentile(99.0),
    }
}

// ── scenario runner ────────────────────────────────────────────────────────

#[derive(Clone)]
struct Scenario {
    command: &'static str,
    dataset_size: usize,
    client_count: usize,
}

async fn run_scenario(
    scenario: &Scenario,
    addr: &str,
    measure_duration: Duration,
    warmup_duration: Duration,
) -> std::io::Result<Stats> {
    let key = format!("{}-{}-{}", scenario.command.to_lowercase(), scenario.dataset_size, scenario.client_count);

    // Seed
    del_key(addr, &key).await?;
    match scenario.command {
        "LRANGE" => seed_list(addr, &key, scenario.dataset_size).await?,
        "XRANGE" => seed_stream(addr, &key, scenario.dataset_size).await?,
        _ => unreachable!(),
    }

    // Build the command frame
    let cmd_frame = match scenario.command {
        "LRANGE" => resp_cmd(&[
            "LRANGE",
            &key,
            "0",
            &(scenario.dataset_size as i64 - 1).to_string(),
        ]),
        "XRANGE" => resp_cmd(&["XRANGE", &key, "-", "+"]),
        _ => unreachable!(),
    };

    // Warmup
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let warmup_handle = tokio::spawn(run_client(
            addr.to_string(),
            cmd_frame.clone(),
            stop_clone,
        ));
        sleep(warmup_duration).await;
        stop.store(true, Ordering::Relaxed);
        let _ = warmup_handle.await;
    }

    // Measurement
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(scenario.client_count);
    for _ in 0..scenario.client_count {
        handles.push(tokio::spawn(run_client(
            addr.to_string(),
            cmd_frame.clone(),
            stop.clone(),
        )));
    }

    let measure_start = Instant::now();
    sleep(measure_duration).await;
    let elapsed = measure_start.elapsed();
    stop.store(true, Ordering::Relaxed);

    let mut all_latencies: Vec<u64> = Vec::new();
    for h in handles {
        match h.await {
            Ok(Ok(lats)) => all_latencies.extend(lats),
            Ok(Err(e)) => eprintln!("client error: {e}"),
            Err(e) => eprintln!("task join error: {e}"),
        }
    }

    Ok(compute_stats(all_latencies, elapsed))
}

// ── output helpers ─────────────────────────────────────────────────────────

fn print_header() {
    println!(
        "{:<8} {:>12} {:>8} {:>12} {:>10} {:>10} {:>10}",
        "CMD", "DATASET", "CLIENTS", "OPS/SEC", "P50 µs", "P95 µs", "P99 µs"
    );
    println!("{}", "-".repeat(74));
}

fn print_row(scenario: &Scenario, stats: &Stats) {
    let ops_per_sec = stats.total_ops as f64 / stats.elapsed_secs;
    println!(
        "{:<8} {:>12} {:>8} {:>12.0} {:>10.1} {:>10.1} {:>10.1}",
        scenario.command,
        scenario.dataset_size,
        scenario.client_count,
        ops_per_sec,
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
    );
}

fn write_csv_header(file: &mut File) -> std::io::Result<()> {
    writeln!(file, "command,dataset_size,clients,ops_per_sec,p50_us,p95_us,p99_us")
}

fn write_csv_row(file: &mut File, scenario: &Scenario, stats: &Stats) -> std::io::Result<()> {
    let ops_per_sec = stats.total_ops as f64 / stats.elapsed_secs;
    writeln!(
        file,
        "{},{},{},{:.0},{:.1},{:.1},{:.1}",
        scenario.command,
        scenario.dataset_size,
        scenario.client_count,
        ops_per_sec,
        stats.p50_us,
        stats.p95_us,
        stats.p99_us,
    )
}

// ── main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let addr = redis_addr();
    let duration = Duration::from_secs(duration_secs());
    let warmup = Duration::from_secs(2);
    let csv_path = output_path();

    // Verify server is reachable before starting.
    if TcpStream::connect(&addr).await.is_err() {
        eprintln!("ERROR: cannot connect to Redis server at {addr}");
        eprintln!("Start the server first:  cargo run");
        std::process::exit(1);
    }

    // Open CSV file (truncate if it already exists).
    let mut csv_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&csv_path)
        .expect("failed to open CSV output file");
    write_csv_header(&mut csv_file).expect("failed to write CSV header");

    let scenarios: Vec<Scenario> = {
        let mut v = Vec::new();
        for cmd in ["LRANGE", "XRANGE"] {
            for &ds in &[1_000usize, 10_000, 50_000] {
                for &cc in &[1usize, 10, 50] {
                    v.push(Scenario { command: cmd, dataset_size: ds, client_count: cc });
                }
            }
        }
        v
    };

    println!("\nRedis Load Test  —  server: {addr}");
    println!("Measurement: {duration_secs}s/scenario  |  Warmup: 2s  |  Scenarios: {}", scenarios.len(), duration_secs = duration_secs());
    println!("CSV output: {csv_path}\n");
    print_header();

    for scenario in &scenarios {
        match run_scenario(&scenario, &addr, duration, warmup).await {
            Ok(stats) => {
                print_row(scenario, &stats);
                write_csv_row(&mut csv_file, scenario, &stats)
                    .expect("failed to write CSV row");
                csv_file.flush().expect("failed to flush CSV");
            }
            Err(e) => {
                eprintln!(
                    "scenario {}/{}/{} failed: {e}",
                    scenario.command, scenario.dataset_size, scenario.client_count
                );
            }
        }
    }

    println!("\nDone. Results saved to {csv_path}");
}
