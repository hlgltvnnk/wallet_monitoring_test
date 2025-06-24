use {
    futures::{sink::SinkExt, stream::StreamExt},
    log::{error, info, warn},
    std::{
        collections::HashMap,
        env,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::{self, time::interval},
    tonic::{Status, service::Interceptor, transport::ClientTlsConfig},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
            SubscribeRequestFilterAccountsFilterLamports, SubscribeUpdate,
            subscribe_request_filter_accounts_filter::Filter,
            subscribe_request_filter_accounts_filter_lamports::Cmp,
        },
        prelude::{CommitmentLevel, SubscribeRequest},
        prost::Message,
    },
    clap::{Parser, ValueEnum},
};

const RUST_LOG_LEVEL: &str = "info";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_PROGRAM_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const STATS_INTERVAL: u64 = 1;

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum Mode {
    #[default]
    Wallet,
    Ata,
}

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long)]
    endpoint: String,

    #[clap(short, long)]
    mode: Mode,

    #[clap(short, long, default_value_t = STATS_INTERVAL)]
    stats_interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    setup_logging();
    info!("Starting to monitor {:?} accounts", args.mode);

    let mut client = setup_client(&args.endpoint).await?;
    info!("Connected to gRPC endpoint");
    let (subscribe_tx, subscribe_rx) = client.subscribe().await?;

    match args.mode {
        Mode::Wallet => send_wallet_subscription_request(subscribe_tx).await?,
        Mode::Ata => send_ata_subscription_request(subscribe_tx).await?,
    }

    info!("Subscription request sent. Listening for updates...");

    process_updates(subscribe_rx, args.stats_interval).await?;

    info!("Stream closed");
    Ok(())
}

fn setup_logging() {
    unsafe { env::set_var("RUST_LOG", RUST_LOG_LEVEL) };
    env_logger::init();
}

async fn setup_client(endpoint: &str) -> Result<GeyserGrpcClient<impl Interceptor>, Box<dyn std::error::Error>> {
    info!("Connecting to gRPC endpoint: {}", endpoint);

    let client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    Ok(client)
}

async fn send_wallet_subscription_request<T>(mut tx: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: SinkExt<SubscribeRequest> + Unpin,
    <T as futures::Sink<SubscribeRequest>>::Error: std::error::Error + 'static,
{
    let mut accounts_filter = HashMap::new();

    accounts_filter.insert(
        "account_monitor".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![SYSTEM_PROGRAM.to_string()],
            filters: vec![
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::Datasize(0)),
                },
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::Lamports(
                        SubscribeRequestFilterAccountsFilterLamports {
                            cmp: Some(Cmp::Gt(0)),
                        },
                    )),
                },
            ],
            nonempty_txn_signature: None,
        },
    );

    tx.send(SubscribeRequest {
        accounts: accounts_filter,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
    .await?;

    Ok(())
}

async fn send_ata_subscription_request<T>(mut tx: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: SinkExt<SubscribeRequest> + Unpin,
    <T as futures::Sink<SubscribeRequest>>::Error: std::error::Error + 'static,
{
    let mut accounts_filter = HashMap::new();

    accounts_filter.insert(
        "account_monitor".to_string(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![TOKEN_PROGRAM.to_string(), TOKEN_PROGRAM_2022.to_string()],
            filters: vec![
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::Datasize(165)),
                },
                SubscribeRequestFilterAccountsFilter {
                    filter: Some(Filter::Lamports(
                        SubscribeRequestFilterAccountsFilterLamports {
                            cmp: Some(Cmp::Gt(0)),
                        },
                    )),
                },
            ],
            nonempty_txn_signature: None,
        },
    );

    // Send subscription request
    tx.send(SubscribeRequest {
        // transactions: accounts_filter,
        accounts: accounts_filter,
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    })
    .await?;

    Ok(())
}

async fn run_stats_loging(
    bytes_counter: Arc<AtomicU64>,
    messages_counter: Arc<AtomicU64>,
    stats_interval: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(stats_interval));
        let mut last_bytes = 0u64;
        let mut last_messages = 0u64;

        loop {
            interval.tick().await;
            let current_bytes = bytes_counter.load(Ordering::Relaxed);
            let current_messages = messages_counter.load(Ordering::Relaxed);

            let bytes_diff = current_bytes - last_bytes;
            let messages_diff = current_messages - last_messages;

            println!("Stats (last {}s):", stats_interval);
            println!("  Messages: {} ({} total)", messages_diff, current_messages);
            println!(
                "  Bytes: {} ({} total)",
                format_bytes(bytes_diff),
                format_bytes(current_bytes)
            );
            println!("  Rate: {}/s", format_bytes(bytes_diff / stats_interval));
            println!();

            last_bytes = current_bytes;
            last_messages = current_messages;
        }
    });

    Ok(())
}

async fn process_updates<S>(mut stream: S, stats_interval: u64) -> Result<(), Box<dyn std::error::Error>>
where
    S: StreamExt<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    let bytes_received = Arc::new(AtomicU64::new(0));
    let messages_received = Arc::new(AtomicU64::new(0));

    run_stats_loging(bytes_received.clone(), messages_received.clone(), stats_interval).await?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                let message_size = msg.encoded_len() as u64;
                bytes_received.fetch_add(message_size, Ordering::Relaxed);
                messages_received.fetch_add(1, Ordering::Relaxed);

                handle_message(msg)
            }
            Err(e) => {
                error!("Error receiving message: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}

fn handle_message(msg: SubscribeUpdate) {
    match msg.update_oneof {
        Some(_) => {
        }
        None => {
            warn!("Empty update received");
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_index])
}
