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

async fn get_count(
    config_db: &String,
    tib_config: tiberius::Config,
    target: &String,
) -> anyhow::Result<i32> {
    let tcp = TcpStream::connect(tib_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(tib_config, tcp.compat_write()).await?;

    let qtext = format!("USE [{}]", config_db);
    let query = Query::new(qtext);
    query.execute(&mut client).await?;

    let qtext = format!(
        "SELECT COUNT(*) FROM [dbo].[SMT_Test] WHERE [Serial_NMBR] = '{}'",
        target
    );

    let query = Query::new(qtext);
    let result = query.query(&mut client).await?;

    if let Some(row) = result.into_row().await? {
        if let Some(x) = row.get::<i32, usize>(0) {
            return Ok(x);
        }
    }

    Err(anyhow::Error::msg("Failed conversion!"))
}

async fn get_count_for_mb(
    config_db: &String,
    tib_config: tiberius::Config,
    target: &str,
    boards: u8,
) -> anyhow::Result<i32> {
    let tcp = TcpStream::connect(tib_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(tib_config, tcp.compat_write()).await?;

    let qtext = format!("USE [{}]", config_db);
    let query = Query::new(qtext);
    query.execute(&mut client).await?;

    let targets: Vec<String> = increment_sn(target, boards)
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
            return Ok(x);
        }
    }

    Err(anyhow::Error::msg("Failed conversion!"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The current working directory will be not the directory of the executable,
    // So we will need to make absolut paths for .\config and .\golden_samples
    let exe_path = env::current_exe().expect("ER: Can't read the directory of the executable!"); // Shouldn't fail.

    // First argument should be the DMC we want to check
    let args: Vec<String> = env::args().collect();
    let target = args.get(1).expect("ER: No argument found!").clone(); // Shouldn't happen

    let boards: u8;
    if let Some(x) = args.get(2) {
        boards = x.parse().unwrap_or(1);
    } else {
        boards = 1;
    }

    // Check if it is a golden sample, if it is then return 'GS'.
    let golden_samples: Vec<String> = load_gs_list(exe_path.with_file_name(GOLDEN));
    if golden_samples.contains(&target) {
        println!("GS: Panel is golden sample");

        return Ok(());
    }

    // Read configuration
    let config = match Config::read(exe_path.with_file_name(CONFIG)) {
        Ok(c) => c,
        Err(e) => {
            println!("{e}");
            std::process::exit(0)
        }
    };

    // Tiberius configuartion:
    let mut tib_config = tiberius::Config::new();

    tib_config.host(config.server);

    tib_config.authentication(tiberius::AuthMethod::sql_server(
        config.username,
        config.password,
    ));

    // Most likely not needed.
    tib_config.trust_cert();
    // Configuration done.

    // The connection might fail sometimes, so we will try 3 times:
    let mut tries = 0;
    let mut result = get_count(&config.database, tib_config.clone(), &target).await;
    while tries < 2 && result.is_err() {
        result = get_count(&config.database, tib_config.clone(), &target).await;
        tries += 1;
    }

    if result.is_err() {
        println!("ER: {}", result.err().unwrap());
        std::process::exit(0)
    }

    let x = result?;

    if x < LIMIT {
        println!("OK: {x}");
    } else if x >= LIMIT_2 || boards < 2 {
        println!("NK: {x}");
    } else {
        // Get the maximum number of failures on the MB
        let mut tries = 0;
        let mut result =
            get_count_for_mb(&config.database, tib_config.clone(), &target, boards).await;
        while tries < 2 && result.is_err() {
            result = get_count_for_mb(&config.database, tib_config.clone(), &target, boards).await;
            tries += 1;
        }

        if result.is_err() {
            println!("ER: {}", result.err().unwrap());
            std::process::exit(0)
        }

        let y = result?;
        if y < LIMIT {
            println!("OK: {y} ({x})");
        } else {
            println!("NK: {y} ({x})");
        }
    }

    Ok(())
}
