use crate::data::*;
use anyhow::Result;
use aprs_parser::{AprsMessage, AprsPacket};
use log::info;
use std::error::Error;

/// Generate an APRS-IS passcode from a given Call Sign
pub fn generate_passcode(callsign: &str) -> Option<String> {
    // Initialize the passcode with 0x73e2
    let mut passcode: u16 = 0x73e2;

    // Strip the SSID off the callsign, and convert it to uppercase
    let base_call = callsign.split('-').next()?.to_uppercase();

    // The algorithm XORs each character of the callsign with the passcode,
    // alternating between shifting the ASCII value 8 bits to the left
    let mut high: bool = true;

    for c in base_call.chars() {
        if high == true {
            passcode ^= (c as u16) << 8;
            high = false;
        } else {
            passcode ^= c as u16;
            high = true;
        }
    }

    return Some(passcode.to_string());
}

pub fn parse_line(data: &str) -> Result<ParsedLine, Box<dyn Error>> {
    let result = AprsPacket::decode_textual(data.as_bytes())?;
    let mut via_strings: Vec<String> = Vec::new();
    for cs in result.via {
        via_strings.push(match cs {
            aprs_parser::Via::Callsign(x, _heard) => format!("{}", x),
            aprs_parser::Via::QConstruct(x) => format!("{}", x.as_textual()),
        });
    }
    let result_data: ParsedAprsData = match ParsedAprsData::from(result.data) {
        ParsedAprsData::Position(x) => ParsedAprsData::Position(x),
        ParsedAprsData::Message(x) => ParsedAprsData::Message(x),
        ParsedAprsData::Status(x) => ParsedAprsData::Status(x),
        ParsedAprsData::MicE(x) => ParsedAprsData::MicE(x),
        ParsedAprsData::Unknown(x) => ParsedAprsData::Unknown(format!("{}", data)),
    };
    Ok(ParsedLine {
        from: result.from.to_string(),
        via: via_strings,
        data: result_data.into(),
    })
}

pub fn print_parsed(data: &ParsedLine) -> Result<(), Box<dyn Error>> {
    match &data.data {
        ParsedAprsData::Message(x) => {
            let via_string: String = data
                .via
                .iter()
                .map(|y| y.to_string() + ", ")
                .collect::<String>();
            let via_string: String = via_string.trim_end_matches(", ").to_string();
            let via_string: String = format!("[via: {}]", via_string);
            let from_string: String = format!("[{}]->[{}]", data.from, x.addressee);
            println!("{0: <30} {1: <50}: {2:}", from_string, via_string, x.text);
        }
        _ => {
            return Ok(());
        }
    }
    Ok(())
}

pub fn print_line(data: &str) -> Result<(), Box<dyn Error>> {
    let result = parse_line(data)?;
    match &result.data {
        ParsedAprsData::Message(x) => {
            let via_string: String = result
                .via
                .iter()
                .map(|y| y.to_string() + ", ")
                .collect::<String>();
            let via_string: String = via_string.trim_end_matches(", ").to_string();
            let via_string: String = format!("[via: {}]", via_string);
            let from_string: String = format!("[{}]->[{}]", result.from, x.addressee);
            println!("{0: <30} {1: <50}: {2:}", from_string, via_string, x.text);
        }
        _ => {
            return Ok(());
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub fn print_messages(data: &str) -> Result<(), Box<dyn Error>> {
    let result = AprsPacket::decode_textual(data.as_bytes())?;
    let callsign = result.from;
    let data = result.data;
    match data {
        #[allow(unused_variables)]
        aprs_parser::AprsData::Message(AprsMessage {
            to: destination,
            addressee: addressee_bytes,
            text: text_bytes,
            id,
        }) => {
            let addressee =
                std::str::from_utf8(&addressee_bytes).unwrap_or_else(|_| "<ERROR PARSING UTF8>");
            let text = std::str::from_utf8(&text_bytes).unwrap_or_else(|_| "<ERROR PARSING UTF8>");
            let fmt = format!(
                "[{}]->[{}] [{}]: {}",
                callsign, destination, addressee, text
            );
            info!("{}", fmt);
        }
        _ => {}
    };
    Ok(())
}
