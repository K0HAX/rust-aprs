use clap::Parser;
use clap::ValueEnum;
use log::{error, info};

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
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
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

async fn main_loop(aprs_client: libk0hax_aprs::client::AprsClient, db: SqliteDb) {
    loop {
        let parsed_line = match aprs_client.read_line().await {
            Ok(x) => x,
            Err(x) => {
                error!("{}", x);
                continue;
            }
        };
        let db_result = db.insert_aprs_line(&parsed_line);
        match db_result {
            Ok(_) => info!("Parsed DB result!"),
            Err(e) => error!("DB Result Error: {}", e),
        }
        libk0hax_aprs::utils::print_parsed(&parsed_line).unwrap();
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

    let my_callsign = args.callsign;
    let client_hostname = "rotate.aprs.net";
    let client_port: u16 = 10152;

    let db_path = "aprs.sqlite";
    let db = SqliteDb::new(db_path);
    let _ = db.create_db();
    let my_client =
        libk0hax_aprs::client::AprsClient::new(client_hostname, client_port, &my_callsign).await;

    println!("Server Address: {:?}", my_client.get_addr());
    // Begin main loop!
    main_loop(my_client, db).await;

    Ok(())
}
