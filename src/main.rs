use clap::Parser;
use clap::ValueEnum;
use ctrlc;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{sleep, Duration};

use std::error::Error;

// Error handling
use anyhow::Result;

mod sqlite;
use sqlite::SqliteDb;

/// Timestamp enum for logging
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[allow(non_camel_case_types)]
pub enum LogTimestamp {
    /// Timestamps off
    none,

    /// Timestamps in seconds
    sec,

    /// Timestamps in milliseconds
    ms,

    /// Timestamps in microseconds
    us,

    /// Timestamps in nanoseconds
    ns,
}

/// APRS Firehose Client
///
/// This program is an APRS client in the proof of concept stage.
///
/// Copyright © 2024 Michael Englehorn
///
/// This program is free software: you can redistribute it and/or modify
/// it under the terms of the GNU General Public License as published by
/// the Free Software Foundation, either version 3 of the License, or
/// (at your option) any later version.
///
/// This program is distributed in the hope that it will be useful,
/// but WITHOUT ANY WARRANTY; without even the implied warranty of
/// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/// GNU General Public License for more details.
///
/// You should have received a copy of the GNU General Public License
/// along with this program.  If not, see <https://www.gnu.org/licenses/>.
#[derive(Parser, Debug)]
#[command(version, about, verbatim_doc_comment)]
struct Args {
    /// Callsign to connect using
    callsign: String,

    /// Increase message verbosity
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbosity: u8,

    /// Silence all output
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// Prepend log lines with a timestamp
    #[arg(short, long, default_value_t = LogTimestamp::none, value_enum)]
    timestamp: LogTimestamp,
}

#[derive(Clone)]
struct AsyncLine {
    line: Arc<Mutex<libk0hax_aprs::data::ParsedLine>>,
}

impl AsyncLine {
    fn insert_aprs_line(&self, db: SqliteDb) {
        let line_handle = Arc::clone(&self.line);

        tokio::spawn(async move {
            let parsed_line = line_handle.lock().await; // Get exclusive access to the line
            let db_result = db.insert_aprs_line(&parsed_line);
            match db_result {
                Ok(_) => info!("Parsed DB result!"),
                Err(e) => error!("DB Result Error: {}", e),
            }
        });
    }

    fn print_parsed(&self) {
        let line_handle = Arc::clone(&self.line);

        tokio::spawn(async move {
            let parsed_line = line_handle.lock().await; // Get exclusive access to the line
            libk0hax_aprs::utils::print_parsed(&parsed_line).unwrap();
        });
    }

    fn new(line: libk0hax_aprs::data::ParsedLine) -> Self {
        AsyncLine {
            line: Arc::new(Mutex::new(line)),
        }
    }
}

async fn main_loop(
    aprs_client: libk0hax_aprs::client::AprsClient,
    tx: mpsc::Sender<AsyncLine>,
    counter_arc: Arc<RwLock<u64>>,
    mut ctrlc_rx: mpsc::Receiver<()>,
) {
    loop {
        match ctrlc_rx.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {
                // No ctrl-c found yet, do nothing
                ()
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // this should be unreachable
                break;
            }
            Ok(()) => {
                println!("Ctrl-C Received! Breaking out of main loop!");
                break;
            }
        };
        let parsed_line = match aprs_client.read_line().await {
            Ok(x) => x,
            Err(x) => {
                error!("{}", x);
                continue;
            }
        };
        let async_line = AsyncLine::new(parsed_line);
        let _ = tx.send(async_line.clone()).await;
        //async_line.insert_aprs_line(db.clone());
        let mut counter = counter_arc.write().await;
        *counter += 1;
        drop(counter);
    }
}

async fn db_loop(
    db: SqliteDb,
    rx: Arc<RwLock<mpsc::Receiver<AsyncLine>>>,
    counter_arc: Arc<RwLock<u64>>,
) {
    let mut handles = Vec::new();
    for i in 0..3 {
        let counter_outer = counter_arc.clone();
        let db_outer = db.clone();
        let rx_outer = rx.clone();
        handles.push((
            i,
            tokio::spawn(async move {
                let db_inner = db_outer.clone();
                while let Some(async_line) = {
                    let rx_inner = rx_outer.clone();
                    let mut rx = rx_inner.write().await;
                    let x = rx.recv().await;
                    drop(rx);
                    x
                } {
                    let db_inner = db_inner.clone();
                    let counter_job = counter_outer.clone();
                    let parsed_line = async_line.line.lock().await;
                    let db_result = db_inner.insert_aprs_line(&parsed_line);
                    match db_result {
                        Ok(_) => {
                            info!("Parsed DB result!");
                            let mut counter = counter_job.write().await;
                            *counter += 1;
                            drop(counter);
                        }
                        Err(e) => {
                            let mut counter = counter_job.write().await;
                            *counter += 1;
                            drop(counter);
                            error!("DB Result Error: {}", e)
                        }
                    }
                    //async_line.insert_aprs_line(db_inner);
                }
            }),
        ));
    }
    for (i, handle) in handles {
        let _ = handle.await.expect("Panic in task");
        println!("DB [{}] Task Finished!", i);
    }
}

async fn log_loop(parse_counter_arc: Arc<RwLock<u64>>, insert_counter_arc: Arc<RwLock<u64>>) {
    loop {
        let parse_counter = parse_counter_arc.read().await;
        let insert_counter = insert_counter_arc.read().await;
        println!("Parsed: {} | Inserted: {}", parse_counter, insert_counter);
        drop(parse_counter);
        drop(insert_counter);
        sleep(Duration::from_secs(60)).await;
    }
}

#[tokio::main]
#[allow(unreachable_code)]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    // Set up logging
    let verbose = args.verbosity as usize;
    let quiet = args.quiet;
    let ts = match args.timestamp {
        LogTimestamp::none => stderrlog::Timestamp::Off,
        LogTimestamp::sec => stderrlog::Timestamp::Second,
        LogTimestamp::ms => stderrlog::Timestamp::Millisecond,
        LogTimestamp::us => stderrlog::Timestamp::Microsecond,
        LogTimestamp::ns => stderrlog::Timestamp::Nanosecond,
    };
    stderrlog::new()
        .module(module_path!())
        .quiet(quiet)
        .verbosity(verbose)
        .timestamp(ts)
        .init()
        .unwrap();

    let (db_tx, db_rx) = mpsc::channel(65534);
    let (ctrlc_tx, ctrlc_rx) = mpsc::channel(1);

    ctrlc::set_handler(move || {
        let cc_tx = ctrlc_tx.clone();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _enter = rt.enter();
        rt.spawn(async move {
            cc_tx
                .send(())
                .await
                .expect("Could not send signal on channel.");
        });
    })
    .expect("Error setting Ctrl-C handler");

    let my_callsign = args.callsign;
    let client_hostname = "rotate.aprs.net";
    let client_port: u16 = 10152;

    let db_path = "aprs.sqlite";
    let db = SqliteDb::new(db_path);
    let _ = db.create_db();
    let my_client =
        libk0hax_aprs::client::AprsClient::new(client_hostname, client_port, &my_callsign).await;

    println!("Server Address: {:?}", my_client.get_addr());

    // Create counters
    let parse_counter = Arc::new(RwLock::new(0u64));
    let insert_counter = Arc::new(RwLock::new(0u64));

    let db_rx_arc = Arc::new(RwLock::new(db_rx));

    let sql_insert_counter = insert_counter.clone();

    let mut handles = Vec::new();

    // Begin SQL Loop!
    handles.push(tokio::spawn(async move {
        db_loop(db, db_rx_arc, sql_insert_counter).await;
    }));

    let log_parse_counter = parse_counter.clone();
    let log_insert_counter = insert_counter.clone();
    // Begin print Loop!
    tokio::spawn(async move {
        log_loop(log_parse_counter, log_insert_counter).await;
    });

    let main_parse_counter = parse_counter.clone();
    main_loop(my_client, db_tx, main_parse_counter, ctrlc_rx).await;
    for handle in handles {
        println!("Joining handle!");
        let _ = handle.await.expect("Panic in task");
    }

    {
        let parse_counter = parse_counter.read().await;
        let insert_counter = insert_counter.read().await;
        println!("Parsed: {} | Inserted: {}", parse_counter, insert_counter);
        drop(parse_counter);
        drop(insert_counter);
    }

    Ok(())
}
