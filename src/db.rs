// DB functions for Brewski.

use postgres::{Connection, TlsMode};
use postgres::params::{ConnectParams, Host};

pub fn get_connection(dbhost: String, dbname: String, user: String, password: String) -> Connection {
    let params = ConnectParams::builder()
        .user(user.as_str(), Some(password.as_str()))
        .database(dbname.as_str())
        .port(5432)
        .build(Host::Tcp(dbhost));
    
    //let conn_url = format!("postgresql://{}:{}@{}:5432/{}", user, password, dbhost, dbname);
    //println!("{}", conn_url);
    Connection::connect(params, TlsMode::None).expect("Database connection failure")
}

/// Fetches MQTT topics to subscribe to from the database.
/// Returns all unique `recv_topic` values from the env_sensors table.
/// 0-length vector if there's nothing to fetch.
pub fn get_recv_topics(conn: &Connection) -> Vec<String>  {
    let mut v: Vec<String> = Vec::new();
    for row in conn.query("SELECT DISTINCT recv_topic FROM env_sensors", &[]).unwrap().iter() {
        v.push(row.get(0));
    }
    v
}