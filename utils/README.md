# Proto
Directory where gproto files will be created

# Operational helpers
- Logs, metrics and traces
- Configuration
- Access to AWS services (e.g. S3)

# Examples
## Identify which cas_server owns a particular key
```
cargo run --example infra -- --server-name cas-lb.xetbeta.com:5000 --key bar
Host: 35.89.208.89
Load Stats: SystemStatus { timestamp: "2022-07-06T19:15:00Z", cpu_utilization: 0.3416666833712037 }
Host: 54.245.178.249
Load Stats: SystemStatus { timestamp: "2022-07-06T19:15:00Z", cpu_utilization: 0.2943333333333333 }
Key bar gets hashed to server "54.245.178.249"
```
