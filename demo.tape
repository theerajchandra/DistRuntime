Output assets/demo.gif

Set FontSize 14
Set Width 1200
Set Height 300
Set Padding 20

Type "cargo run -p distruntime-cli -- start-coordinator --listen 127.0.0.1:8787 --metrics-listen 127.0.0.1:9090 > /tmp/distruntime-demo.log 2>&1 &" Enter
Sleep 2s

Type "export DISTRUNTIME_COORDINATOR=http://127.0.0.1:8787" Enter
Sleep 500ms

Type "cargo run -p distruntime-cli -- job status run-01" Enter
Sleep 1.5s

Type "cargo run -p distruntime-cli -- checkpoint list run-01" Enter
Sleep 2s
Sleep 5s
