use anyhow::{anyhow, Result};
use chrono::prelude::*;
use log::debug;
use sqlx::Connection;
use sqlx::MySqlConnection;
use uuid::Uuid;

pub struct ConnectionArc {
    conn: MySqlConnection,
}

impl ConnectionArc {
    pub async fn new(
        hostname: String,
        username: String,
        password: String,
        database: String,
    ) -> Self {
        let connection_string: String =
            format!("mysql://{username}:{password}@{hostname}/{database}");
        ConnectionArc {
            conn: MySqlConnection::connect(&connection_string).await.unwrap(),
        }
    }

    pub async fn insert_aprs_line(&mut self, data: &libk0hax_aprs::data::ParsedLine) -> Result<()> {
        let record_uuid = Uuid::new_v4();
        debug!(
            "[MariaDB::insert_aprs_line] [{}]: {:?}",
            record_uuid.hyphenated().to_string(),
            &data
        );
        let utc_now: DateTime<Utc> = Utc::now();
        // YYYY-MM-DD HH:MM:SS
        let parsed_time: String = utc_now.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let from: String = data.from.clone();
        let via: String = data
            .via
            .clone()
            .iter()
            .map(|x| x.to_string() + ", ")
            .collect::<String>();
        let via: String = via.trim_end_matches(", ").to_string();

        let type_info: u8 = match &data.data {
            libk0hax_aprs::data::ParsedAprsData::Position(x) => {
                let statement_text = "INSERT INTO `position` (`id`, `to`, `timestamp`, `messaging_supported`, `latitude`, `longitude`, `precision`, `symbol_table`, `symbol_code`, `comment`, `cst`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                let statement = sqlx::query(statement_text);
                let conn = &mut self.conn;
                let record_timestamp: Option<String> = match &x.timestamp {
                    Some(y) => Some(y.mariadb_string()),
                    None => None,
                };
                let _ = statement
                    .bind(record_uuid.hyphenated().to_string())
                    .bind(x.to.clone())
                    .bind(record_timestamp)
                    .bind(x.messaging_supported)
                    .bind(x.latitude)
                    .bind(x.longitude)
                    .bind(x.precision)
                    .bind(x.symbol_table.to_string())
                    .bind(x.symbol_code.to_string())
                    .bind(x.comment.clone())
                    .bind(x.cst.clone())
                    .execute(conn)
                    .await?;
                2
            }
            libk0hax_aprs::data::ParsedAprsData::Message(x) => {
                let statement_text = "INSERT INTO `messages` (`id`, `to`, `addressee`, `text`, `msg_id`) VALUES (?, ?, ?, ?, ?)";
                let statement = sqlx::query(statement_text);
                let conn = &mut self.conn;
                match &x.id {
                    Some(y) => {
                        let _ = statement
                            .bind(record_uuid.hyphenated().to_string())
                            .bind(x.to.clone())
                            .bind(x.addressee.clone())
                            .bind(x.text.clone())
                            .bind(y)
                            .execute(conn)
                            .await?;
                    }
                    None => {
                        let _ = statement
                            .bind(record_uuid.hyphenated().to_string())
                            .bind(x.to.clone())
                            .bind(x.addressee.clone())
                            .bind(x.text.clone())
                            .bind(0)
                            .execute(conn)
                            .await?;
                    }
                }
                1
            }
            libk0hax_aprs::data::ParsedAprsData::Status(x) => {
                let statement_text =
                    "INSERT INTO `status` (`id`, `to`, `timestamp`, `comment`) VALUES (?, ?, ?, ?)";
                let statement = sqlx::query(statement_text);
                let conn = &mut self.conn;
                let record_timestamp: Option<String> = match &x.timestamp {
                    Some(y) => Some(y.mariadb_string()),
                    None => None,
                };
                let _ = statement
                    .bind(record_uuid.hyphenated().to_string())
                    .bind(x.to.clone())
                    .bind(record_timestamp)
                    .bind(x.comment.clone())
                    .execute(conn)
                    .await?;
                3
            }
            libk0hax_aprs::data::ParsedAprsData::MicE(x) => {
                let statement_text = "INSERT INTO `MicE` (`id`, `latitude`, `longitude`, `precision`, `message`, `speed`, `course`, `symbol_table`, `symbol_code`, `comment`, `current`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                let statement = sqlx::query(statement_text);
                let conn = &mut self.conn;
                let _ = statement
                    .bind(record_uuid.hyphenated().to_string())
                    .bind(x.latitude)
                    .bind(x.longitude)
                    .bind(x.precision)
                    .bind(x.message.clone())
                    .bind(x.speed)
                    .bind(x.course)
                    .bind(x.symbol_table.to_string())
                    .bind(x.symbol_code.to_string())
                    .bind(x.comment.clone())
                    .bind(x.current)
                    .execute(conn)
                    .await?;
                4
            }
            libk0hax_aprs::data::ParsedAprsData::Unknown(_x) => {
                return Err(anyhow!("Unknown data type").into())
            }
        };
        debug!("[MariaDB::insert_aprs_line] Data Type: {:?}", &type_info);

        {
            let statement_text =
                "INSERT INTO main_data (`id`, `from`, `via`, `type`, `parsed_time`) VALUES (?, ?, ?, ?, ?)";
            let statement = sqlx::query(statement_text);
            let conn = &mut self.conn;
            let _ = statement
                .bind(record_uuid.hyphenated().to_string())
                .bind(from)
                .bind(via)
                .bind(type_info)
                .bind(parsed_time.clone())
                .execute(conn)
                .await?;
        }
        Ok(())
    }

    pub async fn create_tables(&mut self) -> Result<()> {
        let conn = &mut self.conn;
        let mut tx = conn.begin().await?;

        // Drop the tables if they exist
        {
            let statement_text =
                "DROP TABLE IF EXISTS MicE, main_data, messages, position, status, type;";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Create the message table
        {
            let statement_text = "CREATE TABLE `messages` (
                `id`        CHAR(36) NOT NULL PRIMARY KEY,
                `to`        TEXT NOT NULL,
                `addressee` TEXT NOT NULL,
                `text`      TEXT NOT NULL,
                `msg_id`    INTEGER
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Create the position table
        {
            let statement_text = "CREATE TABLE `position` (
                `id`                  CHAR(36) NOT NULL PRIMARY KEY,
                `to`                  TEXT NOT NULL,
                `timestamp`           DATETIME(6),
                `messaging_supported` INTEGER NOT NULL,
                `latitude`            DOUBLE NOT NULL,
                `longitude`           DOUBLE NOT NULL,
                `precision`           DOUBLE NOT NULL,
                `symbol_table`        TEXT NOT NULL,
                `symbol_code`         TEXT NOT NULL,
                `comment`             TEXT NOT NULL,
                `cst`                 TEXT NOT NULL
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Create the Status table
        {
            let statement_text = "CREATE TABLE `status` (
                `id`                  CHAR(36) NOT NULL PRIMARY KEY,
                `to`                  TEXT NOT NULL,
                `timestamp`           DATETIME(6),
                `comment`             TEXT NOT NULL
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Create the MicE table
        {
            let statement_text = "CREATE TABLE `MicE` (
                `id`                  CHAR(36) NOT NULL PRIMARY KEY,
                `latitude`            DOUBLE NOT NULL,
                `longitude`           DOUBLE NOT NULL,
                `precision`           DOUBLE NOT NULL,
                `message`             TEXT NOT NULL,
                `speed`               INTEGER NOT NULL,
                `course`              INTEGER NOT NULL,
                `symbol_table`        TEXT NOT NULL,
                `symbol_code`         TEXT NOT NULL,
                `comment`             TEXT NOT NULL,
                `current`             INTEGER NOT NULL
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Create the Type Lookup table
        {
            let statement_text = "CREATE TABLE `type` (
                `id`                  CHAR(36) NOT NULL PRIMARY KEY,
                `table`             TEXT NOT NULL
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        // Populate the Type Lookup table
        {
            let tables = vec![(1, "messages"), (2, "position"), (3, "status"), (4, "MicE")];
            debug!(
                "[MariaDb::create_db] prepared records to insert into `type` table: {:?}",
                &tables
            );
            let statement_text = "INSERT INTO `type` (id, `table`) VALUES (?, ?)";
            for table in tables {
                let statement = sqlx::query(statement_text);
                let _ = statement
                    .bind(table.0)
                    .bind(table.1)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        // Create the main lookup table
        {
            let statement_text = "CREATE TABLE `main_data` (
                `id`                  CHAR(36) NOT NULL PRIMARY KEY,
                `from`              TEXT NOT NULL,
                `via`                 TEXT NOT NULL,
                `type`                INTEGER NOT NULL,
                `parsed_time`       DATETIME(6)
            )";
            let statement = sqlx::query(statement_text);
            let _ = statement.execute(&mut *tx).await?;
        }

        Ok(())
    }
}
