//! Entry point of the application: listen to bluetooth advertisements
//! and call the sample handlers when appropriate.
use crate::sample_handler::SensorHandler;
use bluer::{Adapter, AdapterEvent, Address, DeviceEvent, DeviceProperty};
use btsensor::bthome::v2::BtHomeV2;
use config_builder::AppConfig;
use futures::{pin_mut, stream::SelectAll, Stream, StreamExt};
use influxdb::Client;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex,
};

mod config_builder;
mod sample_handler;

/// Magic UUID value for advertised weather data, see the definition of the
/// [custom format](https://github.com/pvvx/ATC_MiThermometer/blob/master/README.md#custom-format-all-data-little-endian).
const WEATHER_SAMPLE_UUID_HEADER: u32 = 0x0000fcd2;

/// Setup the InfluxDb connector, wrapped in Arc and (tokio) Mutex, ready for subsequent usage.
fn setup_influx_connection(app_config: &AppConfig) -> Option<Arc<Mutex<Client>>> {
    match app_config.dry_run {
        true => None,
        false => {
            let influx_client = Client::new(&app_config.influx_conn, &app_config.influx_database);
            let influx_client = match &app_config.influx_credentials {
                Some((username, password)) => influx_client.with_auth(username, password),
                _ => influx_client,
            };
            Some(Arc::new(Mutex::new(influx_client)))
        }
    }
}

/// Setup the bluetooth adapter, ready for subsequent usage.
async fn setup_bluetooth_adapter(
) -> Result<(impl Stream<Item = AdapterEvent>, Adapter), bluer::Error> {
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    println!(
        "Discovering devices using Bluetooth adapater {}",
        adapter.name()
    );
    adapter.set_powered(true).await?;

    let adapter_events = adapter.discover_devices_with_changes().await?;
    Ok((adapter_events, adapter))
}

/// Handle an event linked to the bluetooth adapter, optionaly return a stream of device events.
async fn handle_adapter_evt<'a>(
    adapter_event: AdapterEvent,
    adapter: &Adapter,
    app_config: &'a AppConfig,
) -> Result<Option<impl Stream<Item = (DeviceEvent, Address)>>, bluer::Error> {
    let now = chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
    match adapter_event {
        AdapterEvent::DeviceAdded(addr) => {
            if let Some(room) = app_config.sensors_names.get(&addr) {
                println!("{now} Device {addr} found (room: {room})");
                let device = adapter.device(addr)?;
                let device_events = device.events().await?;
                let device_events = device_events.map(move |e| (e, addr));
                Ok(Some(device_events))
            } else {
                Ok(None)
            }
        }
        AdapterEvent::DeviceRemoved(addr) => {
            match app_config.sensors_names.get(&addr) {
                Some(room) => {
                    println!("{now} Device {addr} removed (room: {room})")
                }
                None => {}
            };
            Ok(None)
        }
        AdapterEvent::PropertyChanged(_) => Ok(None),
    }
}

/// Handle a PropertyChanged event on a bluetooth device: filter the stream,
/// looking for data advertisement with the correct UUID.
async fn handle_dev_changed_prop_evt(
    changed_property: DeviceProperty,
    sender: &mut Sender<BtHomeV2>,
) {
    if let DeviceProperty::ServiceData(service_data) = changed_property {
        for (uuid, raw_sample) in &service_data {
            if uuid.as_fields().0 == WEATHER_SAMPLE_UUID_HEADER {
                match BtHomeV2::decode(raw_sample) {
                    Ok(bthome) => {
                        if let Err(e) = sender.send(bthome).await {
                            println!("{e}");
                        };
                    }
                    Err(e) => println!("{e}"),
                };
            }
        }
    }
}

/// Run the whole application.
#[tokio::main(flavor = "current_thread")]
async fn main() -> bluer::Result<()> {
    let app_config = AppConfig::get_from_cli_inputs().unwrap();
    let influx_client = setup_influx_connection(&app_config);

    let mut channels = HashMap::new();
    for (addr, room) in &app_config.sensors_names {
        let (send, recv) = mpsc::channel(16);
        channels.insert(addr, send);
        let mut sensor_handler = SensorHandler::new(
            *addr,
            room.to_owned(),
            recv,
            influx_client.clone(),
            app_config.influx_measurement.clone(),
            app_config.be_verbose,
        );
        tokio::spawn(async move { sensor_handler.run().await });
    }

    let mut device_events = SelectAll::new();
    let (adapter_events, adapter) = setup_bluetooth_adapter().await?;
    pin_mut!(adapter_events);

    loop {
        tokio::select! {
            Some(adapter_evt) = adapter_events.next() => {
                // Handle some new event related to the bluetooth adapter
                if let Some(new_dev_evts) = handle_adapter_evt(adapter_evt, &adapter, &app_config).await?{
                    // Push the device events stream to tokio
                    device_events.push(new_dev_evts);
                }
            },
            Some((DeviceEvent::PropertyChanged(prop), addr)) = device_events.next() => {
                // Handle a new event related to a linked device
                if let Some(sender)=channels.get_mut(&addr){
                    handle_dev_changed_prop_evt(prop, sender).await;
                }
            },
            else => break
        }
    }
    Ok(())
}
