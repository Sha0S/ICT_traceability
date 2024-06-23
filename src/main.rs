#![allow(non_snake_case)]

use ini::Ini;
use std::{env, fs, path::PathBuf};
use tiberius::{Client, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/*
usage:
config.db {Serial_NMBR} (BoardsOnPanel)

return values:
GS - golden sample
OK {TimesFailed} (TimesMbTested) - Panel OK for testing
NK {TimesFailed} (TimesMbTested) - Panel NOK for testing
ER {Error message} - Program error
*/

static CONFIG: &str = "config.ini";
static GOLDEN: &str = "golden_samples";

static LIMIT: i32 = 3;
static LIMIT_2: i32 = 6;

#[derive(Default)]
struct Config {
    server: String,
    database: String,
    password: String,
    username: String,
}

impl Config {
    fn read(path: PathBuf) -> anyhow::Result<Config> {
        let mut c = Config::default();

        if let Ok(config) = Ini::load_from_file(path.clone()) {
            if let Some(jvserver) = config.section(Some("JVSERVER")) {
                // mandatory fields:
                if let Some(server) = jvserver.get("SERVER") {
                    c.server = server.to_owned();
                }
                if let Some(password) = jvserver.get("PASSWORD") {
                    c.password = password.to_owned();
                }
                if let Some(username) = jvserver.get("USERNAME") {
                    c.username = username.to_owned();
                }
                if let Some(database) = jvserver.get("DATABASE") {
                    c.database = database.to_owned();
                }

                if c.server.is_empty()
                    || c.password.is_empty()
                    || c.username.is_empty()
                    || c.database.is_empty()
                {
                    return Err(anyhow::Error::msg(
                        "ER: Missing fields from configuration file!",
                    ));
                }
            } else {
                return Err(anyhow::Error::msg("ER: Could not find [JVSERVER] field!"));
            }
        } else {
            return Err(anyhow::Error::msg(format!(
                "ER: Could not read configuration file! [{}]",
                path.display()
            )));
        }

        Ok(c)
    }
}

fn load_gs_list(path: PathBuf) -> Vec<String> {
    let mut list = Vec::new();

    if let Ok(fileb) = fs::read_to_string(path) {
        list = fileb.lines().map(|f| f.to_owned()).collect();
    }

    list
}

fn increment_sn(start: &str, boards: u8) -> Vec<String> {
    // VLLDDDxxxxxxx*
    // x is 7 digits -> u32
    let mut ret = Vec::with_capacity(boards as usize);
    ret.push(start.to_string());

    let sn = &start[6..13].parse::<u32>().expect("ER: Parsing error");

    for i in 1..boards {
        let nsn = sn + i as u32;
        let mut next_sn = start.to_string();
        next_sn.replace_range(6..13, &format!("{:07}", nsn));
        ret.push(next_sn);
    }

    ret
}

async fn connect(
    tib_config: tiberius::Config,
) -> anyhow::Result<tiberius::Client<tokio_util::compat::Compat<TcpStream>>> {
    let tcp = TcpStream::connect(tib_config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let client = Client::connect(tib_config, tcp.compat_write()).await?;

    Ok(client)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The current working directory will be not the directory of the executable,
    // So we will need to make absolut paths for .\config and .\golden_samples
    let exe_path = env::current_exe().expect("ER: Can't read the directory of the executable!"); // Shouldn't fail.

    // Read configuration
    let config = match Config::read(exe_path.with_file_name(CONFIG)) {
        Ok(c) => c,
        Err(e) => {
            println!("{e}");
            std::process::exit(0)
        }
    };

    // First argument should be the DMC we want to check
    let args: Vec<String> = env::args().collect();

    let target;
    if let Some(x) = args.get(1) {
        target = x.to_owned();
    } else {
        println!("ER: No argument found!");
        return Ok(());
    }

    let boards: u8;
    if let Some(x) = args.get(2) {
        boards = x.parse().unwrap_or(1);
    } else {
        boards = 1;
    }

    // Check if it is a golden sample, if it is then return 'GS'.
    let golden_samples: Vec<String> = load_gs_list(exe_path.with_file_name(GOLDEN));
    if golden_samples.contains(&target) {
        println!("GS: Panel is a golden sample");
        return Ok(());
    }

    // Tiberius configuartion:
    let mut tib_config = tiberius::Config::new();
    tib_config.host(config.server);
    tib_config.authentication(tiberius::AuthMethod::sql_server(
        config.username,
        config.password,
    ));
    tib_config.trust_cert(); // Most likely not needed.
    // Configuration done.

    // Connect to the DB:
    let mut client_tmp = connect(tib_config.clone()).await;
    let mut tries = 0;
    while client_tmp.is_err() && tries < 3 {
        client_tmp = connect(tib_config.clone()).await;
        tries += 1;
    }

    if client_tmp.is_err() {
        println!("ER: Connection to DB failed!");
        return Ok(());
    }
    let mut client = client_tmp?;

    // USE [DB]
    let qtext = format!("USE [{}]", config.database);
    let query = Query::new(qtext);
    query.execute(&mut client).await?;

    // QUERY #1:

    let qtext = format!(
        "SELECT COUNT(*) FROM [dbo].[SMT_Test] WHERE [Serial_NMBR] = '{}'",
        target
    );

    let query = Query::new(qtext);
    let result = query.query(&mut client).await?;

    let tested_total;
    if let Some(row) = result.into_row().await? {
        if let Some(x) = row.get::<i32, usize>(0) {
            tested_total = x;
            if tested_total < LIMIT {
                println!("OK: {tested_total}");
                return Ok(());
            } else if tested_total >= LIMIT_2 {
                println!("NK: {tested_total}");
                return Ok(());
            }
        } else {
            println!("ER: Q#1 Parsing error.");
            return Ok(());
        }
    } else {
        println!("ER: Q#1 result is none.");
        return Ok(());
    }

    // Check each panel if LIMIT <= tested_total < LIMIT_2
    // No single board should have 'failed' LIMIT times
    // QUERY #2:

    let targets: Vec<String> = increment_sn(&target, boards)
        .iter()
        .map(|f| format!("'{f}'"))
        .collect();
    let targets_string = targets.join(", ");

    let qtext = format!(
        "SELECT COUNT(*) AS Fails
        FROM [dbo].[SMT_Test]
        WHERE [Serial_NMBR] IN ({})
        AND [Result] = 'Failed'
        GROUP BY [Serial_NMBR]
		ORDER BY Fails DESC;",
        targets_string
    );

    let query = Query::new(qtext);
    let result = query.query(&mut client).await?;

    if let Some(row) = result.into_row().await? {
        if let Some(x) = row.get::<i32, usize>(0) {
            if x >= LIMIT {
                println!("NK: {x} ({tested_total})");
                return Ok(());
            } else {
                println!("OK: {x} ({tested_total})");
                return Ok(());
            }
        } else {
            println!("ER: Q#2 Parsing error.");
            return Ok(());
        }
    } else {
        println!("OK: 0 ({tested_total})"); // Q#2 will return NONE, if the MB has no 'failed' results at all.
        return Ok(());
    }
}
