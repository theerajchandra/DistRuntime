use checkpoint_engine::{CheckpointEngine, CheckpointRegistry};
use coordinator::{CoordinatorServiceImpl, DatasetRegistry, LivenessTracker};
use proto_gen::distruntime::coordinator_service_server::CoordinatorServiceServer;
use std::process::Command;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const HB: Duration = Duration::from_millis(100);

async fn start_coordinator(
    tracker: LivenessTracker,
    registry: DatasetRegistry,
    engine: CheckpointEngine,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{addr}");
    let svc = CoordinatorServiceImpl::new(tracker, registry, engine);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(CoordinatorServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (url, handle)
}

// Multi-threaded: blocking `Command::output` must not stall the runtime worker that serves gRPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_checkpoint_list_sorted_help_and_restore() {
    let tracker = LivenessTracker::new(HB);
    let mut ckpt_reg = CheckpointRegistry::new(0);
    ckpt_reg.record("c1", "job-cli", 0, 300, 1, None, None);
    ckpt_reg.record("c2", "job-cli", 0, 100, 1, None, None);
    ckpt_reg.record("c3", "job-cli", 0, 200, 1, None, None);
    let engine = CheckpointEngine::new().with_registry(ckpt_reg);
    let (url, server) = start_coordinator(tracker, DatasetRegistry::new(), engine).await;

    let bin = env!("CARGO_BIN_EXE_distruntime");

    let help = Command::new(bin).arg("--help").output().unwrap();
    assert!(help.status.success());
    let help_txt = String::from_utf8_lossy(&help.stdout);
    assert!(help_txt.contains("distruntime"));

    let sub_help = Command::new(bin)
        .args(["checkpoint", "restore", "--help"])
        .output()
        .unwrap();
    assert!(sub_help.status.success());
    let sub_txt = String::from_utf8_lossy(&sub_help.stdout);
    assert!(
        sub_txt.contains("registry") || sub_txt.contains("version"),
        "restore help should mention version semantics: {sub_txt}"
    );

    let list = Command::new(bin)
        .args(["--coordinator", &url, "checkpoint", "list", "job-cli"])
        .output()
        .unwrap();
    assert!(
        list.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&list.stderr)
    );
    let out = String::from_utf8_lossy(&list.stdout);
    let versions: Vec<u64> = out
        .lines()
        .filter_map(|line| {
            line.strip_prefix("version=")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|v| v.parse().ok())
        })
        .collect();
    assert_eq!(
        versions,
        vec![1, 2, 3],
        "list must be sorted by version: {out}"
    );

    let restore = Command::new(bin)
        .args(["--coordinator", &url, "checkpoint", "restore", "2"])
        .output()
        .unwrap();
    assert!(
        restore.status.success(),
        "{}",
        String::from_utf8_lossy(&restore.stderr)
    );
    let rout = String::from_utf8_lossy(&restore.stdout);
    assert!(rout.contains("checkpoint_id: c2"));
    assert!(rout.contains("committed_path: checkpoints/c2/committed/"));

    let status = Command::new(bin)
        .args(["--coordinator", &url, "job", "status", "job-cli"])
        .output()
        .unwrap();
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("job-cli"));

    server.abort();
}
