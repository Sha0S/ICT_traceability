use ini::Ini;
use std::{env, fs, path::PathBuf};
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
    auth: String,
}

impl Config {
    fn read_config(&mut self, path: PathBuf) {
        if let Ok(config) = Ini::load_from_file(path.clone()) {
            if let Some(jvserver) = config.section(Some("JVSERVER")) {
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
                    if !port.is_empty() {
                        if let Ok(x) = port.parse::<u16>() {
                            self.port = x;
                        } else {
                            eprintln!("W: Could not parse port number: {port}");
                        }
                    }
                }
                if let Some(auth) = jvserver.get("AUTH") {
                    self.auth = auth.to_owned();
                }
            } else {
                panic!("ERR: Could not find [JVSERVER] field!");
            }
        } else {
            panic!(
                "ERR: Could not read configuration file! [{}]",
                path.display()
            );
        }
    }
}

fn load_gs_list(path: PathBuf) -> Vec<String> {
    let mut list = Vec::new();

    if let Ok(fileb) = fs::read_to_string(path) {
        list = fileb.lines().map(|f| f.to_owned()).collect();
    }

    list
}

async fn get_count(
    config_db: &String,
    tib_config: tiberius::Config,
    target: &String,
) -> anyhow::Result<i32> {
    println!("I: Trying to establish TCP connection");
    let tcp = TcpStream::connect(tib_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    println!("I: Trying to establish client connection");
    let mut client = Client::connect(tib_config, tcp.compat_write()).await?;

    if !config_db.is_empty() {
        let qtext = format!("USE [{}]", config_db);
        println!("> {}", qtext);
        let query = Query::new(qtext);
        query.execute(&mut client).await?;
    }

    let qtext = format!(
        "SELECT COUNT(*) FROM [dbo].[SMT_Test] WHERE [Serial_NMBR] = '{}'",
        target
    );
    println!("> {}", qtext);
    let query = Query::new(qtext);
    let result = query.query(&mut client).await?;

    if let Some(row) = result.into_row().await? {
        if let Some(x) = row.get::<i32, usize>(0) {
            return Ok(x);
        }
    }

    Err(anyhow::Error::msg("Failed conversion!"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The current working directory will be not the directory of the executable,
    // So we will need to make absolut paths for .\config and .\golden_samples
    let exe_path = env::current_exe().expect("Can't read the directory of the executable!"); // Shouldn't fail.

    // First argument should be the DMC we want to check
    let args: Vec<String> = env::args().collect();
    let target = args.get(1).expect("ERR: No argument found!").clone();

    // Check if it is a golden sample, if it is then rturn OK.
    let golden_samples: Vec<String> = load_gs_list(exe_path.with_file_name(GOLDEN));
    if golden_samples.contains(&target) {
        println!("I: Target DMC is a golden sample.");
        return Ok(());
    }

    // Read configuration
    let mut config = Config::default();
    config.read_config(exe_path.with_file_name(CONFIG));

    // Tiberius configuartion:
    let mut tib_config = tiberius::Config::new();

    if config.port != 0 {
        tib_config.port(config.port);
    }

    tib_config.host(config.server);

    if config.auth == "WIN" {
        tib_config.authentication(tiberius::AuthMethod::windows(
            config.username,
            config.password,
        ));
    } else {
        tib_config.authentication(tiberius::AuthMethod::sql_server(
            config.username,
            config.password,
        ));
    }

    // Most likely not needed.
    tib_config.trust_cert();
    // Configuration done.

    // The connection might fail sometimes, so we will try 3 times:
    let mut tries = 0;
    let mut result = get_count(&config.database, tib_config.clone(), &target).await;
    while tries < 2 && result.is_err() {
        println!("ERR: Failed to connect, retrying! {}/2", tries + 1);
        result = get_count(&config.database, tib_config.clone(), &target).await;
        tries += 1;
    }

    let x = result?;
    println!("Result received: {x}");

    if x < LIMIT {
        Ok(())
    } else {
        panic!("Target limit reached!")
    }
}
