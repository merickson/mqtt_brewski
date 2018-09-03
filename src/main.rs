extern crate chrono;
#[macro_use]
extern crate log;
extern crate postgres;
extern crate rumqtt;
extern crate simple_logger;
#[macro_use]
extern crate structopt;

use std::string::{String};
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::Mutex;

use postgres::Connection;
use rumqtt::{MqttOptions, MqttCallback, MqttClient, QoS, Message};
use structopt::StructOpt;

mod db;
mod envpayload;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(short = "i", long = "client-id")]
    client_id: String,

    #[structopt(short = "b", long = "broker")]
    broker: String,

    #[structopt(short = "h", long = "dbhost")]
    dbhost: String,

    #[structopt(short = "d", long = "dbname")]
    dbname: String,

    #[structopt(short = "u", long = "dbuser")]
    dbuser: String,

    #[structopt(short = "p", long = "dbpass")]
    dbpass: String,
}

fn init_mqtt(client_id: String, broker: String, msg_chn: &Sender<envpayload::EnvSensorReading>) -> MqttClient {
    let client_options = MqttOptions::new()
        .set_keep_alive(5)
        .set_reconnect(3)
        .set_client_id(client_id)
        .set_broker(&broker);

    // The way rumqtt is built winds up breaking on passing the callback to `on_message()`
    // if there's any non-Sync objects contained, *even with the move keyword*.
    let chn_mutex = Mutex::new(msg_chn.clone());

    let callback = move |msg: Message| {
        let payload = Arc::try_unwrap(msg.payload).expect("Can't unwrap payload!");
        let payload_string = String::from_utf8(payload).expect("Can't make a string out of the payload!");
        let reading = envpayload::EnvSensorReading::from_string(payload_string);
        println!("Received message from {:?}", msg.topic.to_string());
        {
            let chn = chn_mutex.lock().unwrap();    
            chn.send(reading).unwrap();
        }
    };

    let mq_cbs = MqttCallback::new().on_message(callback);

    MqttClient::start(client_options, Some(mq_cbs)).expect("Couldn't start")
}

fn subscribe_topics(conn: &Connection, mqtt: &mut MqttClient) {
    let topics_raw = db::get_recv_topics(conn);
    let mut topicv: Vec<(&str, QoS)> = Vec::new();
    for topic in &topics_raw {
        topicv.push((&topic.as_str(), QoS::Level0));
    }

    mqtt.subscribe(topicv).expect("MQTT Subscription failure");
    debug!("subscribed to topics {:?}", topics_raw);
}

fn main() {
    simple_logger::init().unwrap();
    info!("mqtt_brewski begins. Prepare the cider!");
    let opt = Opt::from_args();
    let conn = db::get_connection(opt.dbhost, opt.dbname, opt.dbuser, opt.dbpass);

    let (msg_tx, msg_rx): (Sender<envpayload::EnvSensorReading>, Receiver<envpayload::EnvSensorReading>) = mpsc::channel();
    let mut request = init_mqtt(opt.client_id, opt.broker, &msg_tx);
    subscribe_topics(&conn, &mut request);

    info!("initialization complete.");
    loop {
        let msg = msg_rx.recv().unwrap();
        msg.log_to_database(&conn);
    }
}
