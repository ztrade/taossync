# taossync
sync taosdb stable between two server

## config

```
src:
  uri: root:123456@tcp(127.0.0.1:6030)/
  db: hft
dst:
  uri: root:123456@tcp(remotehost:6030)/
  db: hft
stables:
  - trade

```
