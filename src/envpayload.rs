// Environmental payloads for Brewski.

use std::str::FromStr;

use chrono::prelude::{DateTime, Utc};
use postgres::Connection;

#[derive(Debug)]
pub struct EnvSensor {
    pub name: String,
}

#[derive(Debug)]
pub struct EnvSensorReading {
    pub sensor: EnvSensor,
    pub timestamp: DateTime<Utc>,
    pub temperature: f32,
    pub pressure: f32,
    pub rel_humid: f32,
    pub lux: f32,
}

impl EnvSensorReading {
    pub fn new() -> EnvSensorReading {
        EnvSensorReading{
            sensor: EnvSensor{name: "".to_string()}, 
            timestamp: Utc::now(), 
            temperature: 0.,
            pressure: 0.,
            rel_humid: 0.,
            lux: 0.,}
    }

    pub fn from_string(reading: String) -> EnvSensorReading {
        let mut tokens = Vec::new();
        let iter = reading.as_str().split_whitespace();

        // We're going to assume the format follows:
        // 1. sensor name
        // 2. temperature in c
        // 3. pressure in kPa
        // 4. relative humidity
        // 5. lux
        for token in iter {
            tokens.push(token);
        }
        EnvSensorReading {
            sensor: EnvSensor{
                name: tokens[0].to_string(),
            },
            timestamp: Utc::now(),
            temperature: f32::from_str(tokens[1]).unwrap(),
            pressure: f32::from_str(tokens[2]).unwrap(),
            rel_humid: f32::from_str(tokens[3]).unwrap(),
            lux: f32::from_str(tokens[4]).unwrap(),
        }
    }

    pub fn log_to_database(&self, conn: &Connection) {
        let rows_updated = conn.execute("INSERT INTO env_reading
            (sensor, temp_c, pressure, humidity, lux)
            values (
                (select id from env_sensors where name = $1),
                $2, $3, $4, $5
            )", &[&self.sensor.name,
                  &self.temperature,
                  &self.pressure,
                  &self.rel_humid,
                  &self.lux,
            ]).unwrap();
        if rows_updated < 1 {
            panic!("No rows updated for logging!");
        }
    }

    pub fn temperature_f(&self) -> f32 {
        self.temperature * 1.8 + 32.0
    }

    pub fn pressure_bar(&self) -> f32 {
        self.pressure / 100.0
    }
}