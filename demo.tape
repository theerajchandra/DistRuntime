Output assets/demo.gif

Set FontSize 14
Set Width 1200
Set Height 760
Set Padding 20

Type "./target/release/distruntime start-coordinator --heartbeat-interval-ms 120 >/tmp/dtr.log 2>&1 &" Enter
Sleep 1s

Type "./target/release/distruntime-demo http://127.0.0.1:8787" Enter
Sleep 40s

Sleep 3s
