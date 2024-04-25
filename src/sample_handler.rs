use bluer::Address;
use btsensor::bthome::v2::{BtHomeV2, Element};
use chrono::{DateTime, Utc};
use influxdb::{Client, InfluxDbWriteable};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

/// Battery data.
#[derive(Copy, Clone, Debug)]
struct Battery {
    voltage: f32,
    level: u8,
}

/// Deserialized data from the sensors with additional metadata.
#[derive(Debug)]
struct Sample<'a> {
    timestamp: DateTime<Utc>,
    sensor_addr: Address,
    room: &'a str,
    temperature: f32,
    humidity: f32,
    battery: Battery,
}

#[derive(Copy, Clone, Debug)]
struct MeteoSample {
    temperature: f32,
    humidity: f32,
    battery_level: u8,
}

#[derive(Copy, Clone, Debug)]
struct VoltageSample {
    battery_voltage: f32,
}

#[derive(Copy, Clone)]
enum BtHomeV2Sample {
    Meteo(MeteoSample),
    Voltage(VoltageSample),
}

impl TryFrom<BtHomeV2> for BtHomeV2Sample {
    type Error = ();

    fn try_from(value: BtHomeV2) -> Result<Self, Self::Error> {
        let mut temperature = None;
        let mut humidity = None;
        let mut battery_level = None;
        let mut battery_voltage = None;
        for element in value.elements {
            match element {
                Element::TemperatureSmall(elt_temperature) => {
                    temperature = Some(1e-2 * f32::from(elt_temperature))
                }
                Element::Humidity(elt_humidity) => humidity = Some(1e-2 * f32::from(elt_humidity)),
                Element::Battery(elt_battery) => battery_level = Some(elt_battery),
                Element::VoltageSmall(elt_voltage) => {
                    battery_voltage = Some(1e-3 * f32::from(elt_voltage))
                }
                _ => {}
            }
        }
        if let (Some(temperature), Some(humidity), Some(battery_level)) =
            (temperature, humidity, battery_level)
        {
            return Ok(BtHomeV2Sample::Meteo(MeteoSample {
                temperature,
                humidity,
                battery_level,
            }));
        }
        if let Some(battery_voltage) = battery_voltage {
            return Ok(BtHomeV2Sample::Voltage(VoltageSample { battery_voltage }));
        }

        Err(())
    }
}

impl<'a> From<(MeteoSample, VoltageSample, DateTime<Utc>, Address, &'a str)> for Sample<'a> {
    fn from(
        (meteo, voltage, timestamp, sensor_addr, room): (
            MeteoSample,
            VoltageSample,
            DateTime<Utc>,
            Address,
            &'a str,
        ),
    ) -> Self {
        Sample {
            timestamp,
            sensor_addr,
            room,
            temperature: meteo.temperature,
            humidity: meteo.humidity,
            battery: Battery {
                voltage: voltage.battery_voltage,
                level: meteo.battery_level,
            },
        }
    }
}

/// InfluxDB structure for a new point (single data record).
#[derive(Debug, InfluxDbWriteable)]
struct InfluxPoint<'a> {
    time: DateTime<Utc>,
    #[influxdb(tag)]
    sensor: String,
    #[influxdb(tag)]
    room: &'a str,
    temperature: f32,
    humidity: f32,
    battery_voltage: f32,
    battery_level: i32,
}

impl<'a> From<&Sample<'a>> for InfluxPoint<'a> {
    fn from(measurement: &Sample<'a>) -> Self {
        Self {
            time: measurement.timestamp,
            sensor: measurement.sensor_addr.to_string(),
            room: measurement.room,
            temperature: measurement.temperature,
            humidity: measurement.humidity,
            battery_voltage: measurement.battery.voltage,
            battery_level: measurement.battery.level.into(),
        }
    }
}

pub struct SensorHandler {
    sensor_addr: Address,
    room: String,
    recv: Receiver<BtHomeV2>,
    influx_client: Option<Arc<Mutex<Client>>>,
    influx_measurement: String,
    be_verbose: bool,
    last_voltage: Option<VoltageSample>,
}

impl SensorHandler {
    pub fn new(
        sensor_addr: Address,
        room: String,
        recv: Receiver<BtHomeV2>,
        influx_client: Option<Arc<Mutex<Client>>>,
        influx_measurement: String,
        be_verbose: bool,
    ) -> Self {
        Self {
            sensor_addr,
            room,
            recv,
            influx_client,
            influx_measurement,
            be_verbose,
            last_voltage: None,
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.recv.recv().await {
            match BtHomeV2Sample::try_from(msg) {
                Ok(BtHomeV2Sample::Meteo(meteo)) => {
                    if self.be_verbose {
                        println!("Room {}: {meteo:?}", self.room);
                    }

                    if let Some(voltage) = self.last_voltage {
                        let sample = Sample::from((
                            meteo,
                            voltage,
                            Utc::now(),
                            self.sensor_addr,
                            self.room.as_str(),
                        ));

                        if let Some(influx_client) = &self.influx_client {
                            if self.be_verbose {
                                println!("Room {}: send {sample:?}", self.room);
                            }
                            let influx_client = influx_client.clone();
                            let influx_client = influx_client.lock().await;
                            let point = InfluxPoint::from(&sample);
                            let query = point.into_query(&self.influx_measurement);
                            influx_client.query(query).await.unwrap();
                        } else {
                            if self.be_verbose {
                                println!("Room {}: dry-run {sample:?}", self.room);
                            }
                        }
                    }
                }
                Ok(BtHomeV2Sample::Voltage(voltage)) => {
                    if self.be_verbose {
                        println!("Room {}: {voltage:?}", self.room);
                    }
                    self.last_voltage = Some(voltage);
                }
                Err(()) => {
                    if self.be_verbose {
                        println!("Room {}: cannot interpret data", self.room);
                    }
                }
            }
        }
    }
}
