# Quique
quique[ˈkiːke] is in-memory queue system

<!-- TODO -->
## How-to
### Run server
```
$ cargo run --bin qq-server
...
quique server listening on 127.0.0.1:7001
```

### Run clients
Create and produce message to topic
```
$ cargo run --bin qq-cli create --topic sample
...
$ cargo run --bin qq-cli produce --topic sample --key topic --data "hello"
```

Consume message from topic
```
$ cargo run --bin qq-cli consume --topic sample --key topic
value=hello
```
