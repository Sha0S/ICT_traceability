use std::{env, fs, path::Path};
use ini::Ini;
use tiberius::{Client, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

static CONFIG: &str = "config.ini";
static GOLDEN: &str = "golden_samples";
static LIMIT: i32 = 6;

#[derive(Default)]
struct Config {
    server: String,
    database: String,
    password: String,
    username: String,
    port: u16,
    auth: String
}

impl Config {
    fn read_config(&mut self, path: &str) {
        if let Ok( config )  = Ini::load_from_file(path) {
            if let Some(jvserver)  = config.section(Some("JVSERVER")) {

                // mandatory fields:
                if let Some(server) = jvserver.get("SERVER") {
                    self.server = server.to_owned();
                }
                if let Some(password) = jvserver.get("PASSWORD") {
                    self.password = password.to_owned();
                }
                if let Some(username) = jvserver.get("USERNAME") {
                    self.username = username.to_owned();
                }

                if self.server.is_empty() || self.password.is_empty() || self.username.is_empty() {
                    panic!("ERR: Missing mandatory fields from configuration file!")
                }

                // optional:
                if let Some(database) = jvserver.get("DATABASE") {
                    self.database = database.to_owned();
                }
                if let Some(port) = jvserver.get("PORT") {
                    if let Ok(x) = port.parse::<u16>() {
                        self.port = x;
                    } else {
                        eprintln!("W: Could not parse port number: {port}");
                    }
                }
                if let Some(auth) = jvserver.get("AUTH") {
                    self.auth = auth.to_owned();
                }
            }  else {
                panic!("ERR: Could not find [JVSERVER] field!");
            }
        } else {
            panic!("ERR: Could not read configuration file!");
        }
    }
}

fn load_gs_list(path: &str) -> Vec<String> {
    let mut list = Vec::new();

    let p = Path::new(path);
    if let Ok(fileb) = fs::read_to_string(p) {
        list = fileb.lines().map(|f| f.to_owned()).collect();
    }

    list
}



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("ERR: No argument found!")
    }

    let target = args[1].clone();

    let golden_samples: Vec<String> = load_gs_list(GOLDEN);
    if golden_samples.contains(&target) {
        println!("I: Target DMC is a golden sample.");
        return Ok(())
    }


    let mut config = Config::default();
    config.read_config(CONFIG);

    let mut tib_config = tiberius::Config::new();

    if config.port != 0 {
        tib_config.port(config.port);
    }
    tib_config.host(config.server);

    if config.auth == "WIN" {
        tib_config.authentication(tiberius::AuthMethod::windows(config.username, config.password));
    } else {
        tib_config.authentication(tiberius::AuthMethod::sql_server(config.username, config.password));
    }
    
    tib_config.trust_cert();

    println!("I: Trying to establish TCP connection");
    let tcp = TcpStream::connect(tib_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    println!("I: Trying to establish client connection");
    let mut client = Client::connect(tib_config, tcp.compat_write()).await?;
    
    if !config.database.is_empty() {
        let qtext = format!("USE [{}]", config.database);
        println!("> {}", qtext);
        let query = Query::new(qtext);
        query.execute(&mut client).await?;
    }


    let qtext = format!("SELECT COUNT(*) FROM [dbo].[SMT_Test] WHERE [Serial_NMBR] = '{}'", target);
    println!("> {}", qtext);
    let query = Query::new(qtext);
    let result = query.query(&mut client).await?;

    if let Some(row) = result.into_row().await? {
        if let Some(x) = row.get::<i32, usize>(0) {
            println!("Result is: {x}");
            if x < LIMIT {
                return Ok(())
            } else {
                panic!("ERR: Target limit reached!")
            }
        }
    }

    unreachable!()
}