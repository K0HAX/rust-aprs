use anyhow::{anyhow, Result};
use rusqlite::Connection;
use chrono::prelude::*;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use log::debug;

#[derive(Clone)]
pub struct SqliteDb {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDb {
    pub fn new(path: &str) -> Self {
        let conn = Connection::open(path).unwrap();
        debug!("[SqliteDb::new] Connection opened.");
        SqliteDb {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    pub fn insert_aprs_line(&self, data: &libk0hax_aprs::data::ParsedLine) -> Result<()> {
        let record_uuid = Uuid::new_v4();
        debug!("[SqliteDb::insert_aprs_line] [{}]: {:?}", record_uuid.hyphenated().to_string(), &data);
        let utc_now: DateTime<Utc> = Utc::now();
        let parsed_time: String = utc_now.format("%+").to_string();

        let from: String = data.from.clone();
        let via: String = data
            .via
            .clone()
            .iter()
            .map(|x| x.to_string() + ", ")
            .collect::<String>();
        let via: String = via.trim_end_matches(", ").to_string();

        let conn_handle = Arc::clone(&self.conn);
        let conn = conn_handle.lock().unwrap();

        let type_info: u8 = match &data.data {
            libk0hax_aprs::data::ParsedAprsData::Position(x) => {
                let statement_text = "INSERT INTO `position` (id, `to`, timestamp, messaging_supported, latitude, longitude, precision, symbol_table, symbol_code, comment, cst) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
                let mut statement = conn.prepare_cached(statement_text)?;
                match &x.timestamp {
                    Some(y) => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            y.fmt_string(),
                            x.messaging_supported,
                            x.latitude,
                            x.longitude,
                            x.precision,
                            x.symbol_table.to_string(),
                            x.symbol_code.to_string(),
                            x.comment.clone(),
                            x.cst.clone(),
                        ))?;
                    }
                    None => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            "",
                            x.messaging_supported,
                            x.latitude,
                            x.longitude,
                            x.precision,
                            x.symbol_table.to_string(),
                            x.symbol_code.to_string(),
                            x.comment.clone(),
                            x.cst.clone(),
                        ))?;
                    }
                }
                2
            }
            libk0hax_aprs::data::ParsedAprsData::Message(x) => {
                let statement_text = "INSERT INTO `messages` (`id`, `to`, `addressee`, `text`, `msg_id`) VALUES (?1, ?2, ?3, ?4, ?5)";
                let mut statement = conn.prepare_cached(statement_text)?;
                match &x.id {
                    Some(y) => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            x.addressee.clone(),
                            x.text.clone(),
                            y,
                        ))?;
                    }
                    None => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            x.addressee.clone(),
                            x.text.clone(),
                            0,
                        ))?;
                    }
                }
                1
            }
            libk0hax_aprs::data::ParsedAprsData::Status(x) => {
                let statement_text = "INSERT INTO `status` (`id`, `to`, `timestamp`, `comment`) VALUES (?1, ?2, ?3, ?4)";
                let mut statement = conn.prepare_cached(statement_text)?;
                match &x.timestamp {
                    Some(y) => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            y.fmt_string(),
                            x.comment.clone(),
                        ))?;
                    }
                    None => {
                        let _ = statement.execute((
                            record_uuid.hyphenated().to_string(),
                            x.to.clone(),
                            "",
                            x.comment.clone(),
                        ))?;
                    }
                }
                3
            }
            libk0hax_aprs::data::ParsedAprsData::MicE(x) => {
                let statement_text = "INSERT INTO `MicE` (`id`, `latitude`, `longitude`, `precision`, `message`, `speed`, `course`, `symbol_table`, `symbol_code`, `comment`, `current`) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
                let mut statement = conn.prepare_cached(statement_text)?;
                let _ = statement.execute((
                    record_uuid.hyphenated().to_string(),
                    x.latitude,
                    x.longitude,
                    x.precision,
                    x.message.clone(),
                    x.speed,
                    x.course,
                    x.symbol_table.to_string(),
                    x.symbol_code.to_string(),
                    x.comment.clone(),
                    x.current,
                ))?;
                4
            }
            libk0hax_aprs::data::ParsedAprsData::Unknown(_x) => {
                return Err(anyhow!("Unknown data type").into())
            }
        };
        debug!("[SqliteDb::insert_aprs_line] Data Type: {:?}", &type_info);

        {
            let statement_text =
                "INSERT INTO main_data (id, `from`, via, type, `parsed_time`) VALUES (?1, ?2, ?3, ?4, ?5)";
            let mut statement = conn.prepare_cached(statement_text)?;
            let _ =
                statement.execute((record_uuid.hyphenated().to_string(), from, via, type_info, parsed_time.clone()))?;
        }
        Ok(())
    }

    pub fn create_db(&self) -> Result<()> {
        let conn_handle = Arc::clone(&self.conn);
        let conn = conn_handle.lock().unwrap();
        debug!("[SqliteDb::create_db] Got connection lock");

        // Create the message table
        conn.execute(
            "CREATE TABLE messages (
                `id`        TEXT PRIMARY KEY,
                `to`        TEXT NOT NULL,
                addressee TEXT NOT NULL,
                text      TEXT NOT NULL,
                msg_id    INTEGER
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `messages` table");

        // Create the position table
        conn.execute(
            "CREATE TABLE position (
                id                  TEXT PRIMARY KEY,
                `to`                  TEXT NOT NULL,
                timestamp           TEXT,
                messaging_supported INTEGER NOT NULL,
                latitude            REAL NOT NULL,
                longitude           REAL NOT NULL,
                precision           REAL NOT NULL,
                symbol_table        TEXT NOT NULL,
                symbol_code         TEXT NOT NULL,
                comment             TEXT NOT NULL,
                cst                 TEXT NOT NULL
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `position` table");

        // Create the Status table
        conn.execute(
            "CREATE TABLE status (
                id                  TEXT PRIMARY KEY,
                `to`                  TEXT NOT NULL,
                timestamp           TEXT,
                comment             TEXT NOT NULL
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `status` table");

        // Create the MicE table
        conn.execute(
            "CREATE TABLE MicE (
                id                  TEXT PRIMARY KEY,
                latitude            REAL NOT NULL,
                longitude           REAL NOT NULL,
                precision           REAL NOT NULL,
                message             TEXT NOT NULL,
                speed               INTEGER NOT NULL,
                course              INTEGER NOT NULL,
                symbol_table        TEXT NOT NULL,
                symbol_code         TEXT NOT NULL,
                comment             TEXT NOT NULL,
                current             INTEGER NOT NULL
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `MicE` table");

        // Create the Type Lookup table
        conn.execute(
            "CREATE TABLE `type` (
                id                  TEXT PRIMARY KEY,
                `table`             TEXT NOT NULL
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `type` table");

        // Populate the Type Lookup table
        {
            let tables = vec![(1, "messages"), (2, "position"), (3, "status"), (4, "MicE")];
            debug!("[SqliteDb::create_db] prepared records to insert into `type` table: {:?}", &tables);
            let mut type_statement =
                conn.prepare_cached("INSERT INTO `type` (id, `table`) VALUES (?1, ?2)")?;
            for table in tables {
                type_statement.execute(table)?;
            }
            debug!("[SqliteDb::create_db] Completed building `type` table");
        }

        // Create the main lookup table
        conn.execute(
            "CREATE TABLE main_data (
                id                  TEXT PRIMARY KEY,
                `from`              TEXT NOT NULL,
                via                 TEXT NOT NULL,
                type                INTEGER NOT NULL,
                `parsed_time`       TEXT
            )",
            (), // empty list of parameters.
        )?;
        debug!("[SqliteDb::create_db] Created `main_data` table");

        Ok(())
    }
}
