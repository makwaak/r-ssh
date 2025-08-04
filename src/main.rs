//! rust-pssh: A high-performance parallel SSH executor with JSON logging and timing support

use openssh::{KnownHosts, SessionBuilder};
use tokio::sync::Semaphore;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::{Duration, Instant};
use clap::{Arg, ArgAction, Command};
use serde::Serialize;
use std::sync::Mutex;
use std::collections::VecDeque;

// Struct to hold the result of each SSH command
#[derive(Serialize)]
struct HostResult {
    host: String,
    status: String,
    duration_ms: u128,
    output: Option<String>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let matches = Command::new("rust-pssh")
        .version("1.0")
        .about("Parallel SSH tool in Rust")
        .arg(Arg::new("hostfile")
            .short('f')
            .long("hostfile")
            .action(ArgAction::Set)
            .required(true)
            .help("Path to file with list of hosts"))
        .arg(Arg::new("command")
            .short('c')
            .long("command")
            .action(ArgAction::Set)
            .required(true)
            .help("Command to run on each host"))
        .arg(Arg::new("concurrency")
            .short('n')
            .long("concurrency")
            .action(ArgAction::Set)
            .required(false)
            .default_value("500")
            .help("Maximum number of concurrent SSH connections"))
        .arg(Arg::new("timeout")
            .short('t')
            .long("timeout")
            .action(ArgAction::Set)
            .required(false)
            .default_value("10")
            .help("Timeout per host in seconds"))
        .arg(Arg::new("logfile")
            .short('l')
            .long("logfile")
            .action(ArgAction::Set)
            .required(false)
            .default_value("results.json")
            .help("Path to output JSON log file"))
        .arg(Arg::new("index")
            .long("index")
            .action(ArgAction::Set)
            .required(false)
            .default_value("0")
            .help("Index of current jump host (0, 1, or 2)"))
        .arg(Arg::new("total")
            .long("total")
            .action(ArgAction::Set)
            .required(false)
            .default_value("1")
            .help("Total number of jump hosts to split work between"))
        .get_matches();

    // Extract argument values
    let hostfile = matches.get_one::<String>("hostfile").unwrap();
    let command = matches.get_one::<String>("command").unwrap();
    let concurrency: usize = matches.get_one::<String>("concurrency").unwrap().parse()?;
    let timeout_secs: u64 = matches.get_one::<String>("timeout").unwrap().parse()?;
    let logfile = matches.get_one::<String>("logfile").unwrap();
    let index: usize = matches.get_one::<String>("index").unwrap().parse()?;
    let total: usize = matches.get_one::<String>("total").unwrap().parse()?;

    // Open and read the list of hosts
    let file = File::open(hostfile)?;
    let reader = BufReader::new(file);
    let all_hosts: Vec<String> = reader.lines().filter_map(Result::ok).collect();

    // Filter hosts based on jump host index (distribute work)
    let filtered_hosts: Vec<String> = all_hosts
        .into_iter()
        .enumerate()
        .filter(|(i, _)| i % total == index)
        .map(|(_, h)| h)
        .collect();

    // Shared semaphore to limit concurrent SSH sessions
    let semaphore = Arc::new(Semaphore::new(concurrency));

    // Shared thread-safe result buffer
    let results = Arc::new(Mutex::new(VecDeque::new()));

    // Track all ongoing SSH futures
    let mut futures = FuturesUnordered::new();

    // Spawn async SSH tasks for each assigned host
    for host in filtered_hosts {
        let cmd = command.to_string();
        let semaphore = semaphore.clone();
        let results_clone = results.clone();
        let host_clone = host.clone();

        futures.push(tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await.unwrap();
            let start_time = Instant::now();

            let res = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
                let session = SessionBuilder::default()
                    .known_hosts_check(KnownHosts::Accept)
                    .connect(&host_clone)
                    .await?;

                let output = session.command(cmd).output().await?;
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                session.close().await?;

                results_clone.lock().unwrap().push_back(HostResult {
                    host: host_clone.clone(),
                    status: "success".to_string(),
                    duration_ms: start_time.elapsed().as_millis(),
                    output: Some(stdout),
                    error: None,
                });
                Ok::<_, Box<dyn std::error::Error>>(())
            }).await;

            drop(permit);

            if let Err(e) = res {
                results_clone.lock().unwrap().push_back(HostResult {
                    host: host_clone.clone(),
                    status: "error".to_string(),
                    duration_ms: start_time.elapsed().as_millis(),
                    output: None,
                    error: Some(format!("{:?}", e)),
                });
            }
        }));
    }

    // Await all SSH tasks to complete
    while let Some(_) = futures.next().await {}

    // Write results to JSON file
    let collected = results.lock().unwrap();
    let json = serde_json::to_string_pretty(&*collected)?;
    let mut file = File::create(logfile)?;
    file.write_all(json.as_bytes())?;

    println!("Results written to {}", logfile);

    Ok(())
}

