use crate::utils::generate_passcode;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

/// Timestamp enum
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Timestamp {
    /// Day of month, Hour, Minute in UTC
    DDHHMM(u8, u8, u8),
    /// Hour, Minute, Second in UTC
    HHMMSS(u8, u8, u8),
    /// Unsupported timestamp format
    Unsupported(Vec<u8>),
}

impl Timestamp {
    pub fn fmt_string(&self) -> String {
        let utc_now: DateTime<Utc> = Utc::now();
        let cur_year = utc_now.year();
        let cur_month = utc_now.month();
        let cur_day = utc_now.day();

        match self {
            Timestamp::DDHHMM(x, y, z) => {
                let dt_string = format!("{}-{} {}:{}:00 +0000", cur_year, x, y, z);
                let format_string = "%Y-%j %H:%M:%S %z";
                match DateTime::parse_from_str(&dt_string, format_string) {
                    Ok(x) => x.format("%+").to_string(),
                    Err(_e) => "".to_string(),
                }
            }
            Timestamp::HHMMSS(x, y, z) => {
                let dt_string = format!(
                    "{}-{}-{} {}:{}:{} +0000",
                    cur_year, cur_month, cur_day, x, y, z
                );
                let format_string = "%Y-%m-%d %H:%M:%S %z";
                match DateTime::parse_from_str(&dt_string, format_string) {
                    Ok(x) => x.format("%+").to_string(),
                    Err(_e) => "".to_string(),
                }
            }
            Timestamp::Unsupported(_) => "".to_string(),
        }
    }

    pub fn mariadb_string(&self) -> String {
        let utc_now: DateTime<Utc> = Utc::now();
        let cur_year = utc_now.year();
        let cur_month = utc_now.month();
        let cur_day = utc_now.day();

        match self {
            Timestamp::DDHHMM(x, y, z) => {
                let dt_string = format!("{}-{} {}:{}:00 +0000", cur_year, x, y, z);
                let format_string = "%Y-%j %H:%M:%S %z";
                match DateTime::parse_from_str(&dt_string, format_string) {
                    // YYYY-MM-DD HH:MM:SS
                    Ok(x) => x.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                    Err(_e) => "".to_string(),
                }
            }
            Timestamp::HHMMSS(x, y, z) => {
                let dt_string = format!(
                    "{}-{}-{} {}:{}:{} +0000",
                    cur_year, cur_month, cur_day, x, y, z
                );
                let format_string = "%Y-%m-%d %H:%M:%S %z";
                match DateTime::parse_from_str(&dt_string, format_string) {
                    // YYYY-MM-DD HH:MM:SS
                    Ok(x) => x.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
                    Err(_e) => "".to_string(),
                }
            }
            Timestamp::Unsupported(_) => "".to_string(),
        }
    }
}

impl From<aprs_parser::Timestamp> for Timestamp {
    fn from(item: aprs_parser::Timestamp) -> Self {
        match item {
            aprs_parser::Timestamp::DDHHMM(x, y, z) => Timestamp::DDHHMM(x, y, z),
            aprs_parser::Timestamp::HHMMSS(x, y, z) => Timestamp::HHMMSS(x, y, z),
            aprs_parser::Timestamp::Unsupported(x) => Timestamp::Unsupported(x),
        }
    }
}

/// Parsed APRS Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedAprsMessage {
    pub to: String,
    pub addressee: String,
    pub text: String,
    pub id: Option<Vec<u8>>,
}

impl From<aprs_parser::AprsMessage> for ParsedAprsMessage {
    fn from(item: aprs_parser::AprsMessage) -> Self {
        ParsedAprsMessage {
            to: format!("{}", item.to),
            addressee: std::str::from_utf8(&item.addressee)
                .unwrap_or_else(|_| "<ERROR PARSING UTF8>")
                .to_string(),
            text: std::str::from_utf8(&item.text)
                .unwrap_or_else(|_| "<ERROR PARSING UTF8>")
                .to_string(),
            id: item.id,
        }
    }
}

/// Parsed APRS Position
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedAprsPosition {
    pub to: String,
    pub timestamp: Option<Timestamp>,
    pub messaging_supported: bool,
    pub latitude: f64,
    pub longitude: f64,
    pub precision: f64,
    pub symbol_table: char,
    pub symbol_code: char,
    pub comment: String,
    pub cst: String,
}

impl From<aprs_parser::AprsPosition> for ParsedAprsPosition {
    fn from(item: aprs_parser::AprsPosition) -> Self {
        ParsedAprsPosition {
            to: format!("{}", item.to),
            timestamp: match item.timestamp {
                Some(x) => Some(Timestamp::from(x)),
                None => None,
            },
            messaging_supported: item.messaging_supported,
            latitude: item.latitude.value(),
            longitude: item.longitude.value(),
            precision: item.precision.width(),
            symbol_table: item.symbol_table,
            symbol_code: item.symbol_code,
            comment: std::str::from_utf8(&item.comment)
                .unwrap_or_else(|_| "<ERROR PARSING UTF8>")
                .to_string(),
            cst: format!("{:?}", item.cst),
        }
    }
}

/// Parsed APRS Status
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedAprsStatus {
    pub to: String,
    pub timestamp: Option<Timestamp>,
    pub comment: String,
}

impl From<aprs_parser::AprsStatus> for ParsedAprsStatus {
    fn from(item: aprs_parser::AprsStatus) -> Self {
        ParsedAprsStatus {
            to: format!("{}", item.to),
            timestamp: match item.timestamp() {
                Some(x) => Some(Timestamp::from(x.to_owned())),
                None => None,
            },
            comment: std::str::from_utf8(item.comment())
                .unwrap_or_else(|_| "<ERROR PARSING UTF8>")
                .to_string(),
        }
    }
}

/// Parsed APRS MicE
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedAprsMicE {
    pub latitude: f64,
    pub longitude: f64,
    pub precision: f64,
    pub message: String,
    // speed is in knots
    pub speed: u32,
    // course is in degrees
    pub course: u32,
    pub symbol_table: char,
    pub symbol_code: char,
    pub comment: String,
    pub current: bool,
}

impl From<aprs_parser::AprsMicE> for ParsedAprsMicE {
    fn from(item: aprs_parser::AprsMicE) -> Self {
        ParsedAprsMicE {
            latitude: item.latitude.value(),
            longitude: item.longitude.value(),
            precision: item.precision.width(),
            message: match item.message {
                aprs_parser::mic_e::Message::M0 => "M0".to_string(),
                aprs_parser::mic_e::Message::M1 => "M1".to_string(),
                aprs_parser::mic_e::Message::M2 => "M2".to_string(),
                aprs_parser::mic_e::Message::M3 => "M3".to_string(),
                aprs_parser::mic_e::Message::M4 => "M4".to_string(),
                aprs_parser::mic_e::Message::M5 => "M5".to_string(),
                aprs_parser::mic_e::Message::M6 => "M6".to_string(),
                aprs_parser::mic_e::Message::C0 => "C0".to_string(),
                aprs_parser::mic_e::Message::C1 => "C1".to_string(),
                aprs_parser::mic_e::Message::C2 => "C2".to_string(),
                aprs_parser::mic_e::Message::C3 => "C3".to_string(),
                aprs_parser::mic_e::Message::C4 => "C4".to_string(),
                aprs_parser::mic_e::Message::C5 => "C5".to_string(),
                aprs_parser::mic_e::Message::C6 => "C6".to_string(),
                aprs_parser::mic_e::Message::Emergency => "Emergency".to_string(),
                aprs_parser::mic_e::Message::Unknown => "Unknown".to_string(),
            },
            speed: item.speed.knots(),
            course: item.course.degrees(),
            symbol_table: std::char::from_u32(item.symbol_table as u32).unwrap(),
            symbol_code: std::char::from_u32(item.symbol_code as u32).unwrap(),
            comment: std::str::from_utf8(&item.comment)
                .unwrap_or_else(|_| "<ERROR PARSING UTF8>")
                .to_string(),
            current: item.current,
        }
    }
}

/// Parsed APRS Data
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ParsedAprsData {
    Position(ParsedAprsPosition),
    Message(ParsedAprsMessage),
    Status(ParsedAprsStatus),
    MicE(ParsedAprsMicE),
    Unknown(String),
}

impl From<aprs_parser::AprsData> for ParsedAprsData {
    fn from(item: aprs_parser::AprsData) -> Self {
        match item {
            aprs_parser::AprsData::Position(x) => {
                ParsedAprsData::Position(ParsedAprsPosition::from(x))
            }
            aprs_parser::AprsData::Message(x) => {
                ParsedAprsData::Message(ParsedAprsMessage::from(x))
            }
            aprs_parser::AprsData::Status(x) => ParsedAprsData::Status(ParsedAprsStatus::from(x)),
            aprs_parser::AprsData::MicE(x) => ParsedAprsData::MicE(ParsedAprsMicE::from(x)),
            aprs_parser::AprsData::Unknown(x) => ParsedAprsData::Unknown(format!("{:?}", x)),
        }
    }
}

/// Parsed APRS Line
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ParsedLine {
    pub from: String,
    pub via: Vec<String>,
    pub data: ParsedAprsData,
}

/// Handshake message sent from a client to a server when it first connects,
/// identifying the client.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Handshake {
    pub callsign: String,
    pub passcode: String,
}

impl Handshake {
    pub fn new(callsign: String) -> Handshake {
        Handshake {
            callsign: callsign.clone(),
            passcode: generate_passcode(&callsign).unwrap(),
        }
    }
}
