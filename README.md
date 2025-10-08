# zio-mini-redis

A minimal Redis server implementation in Zig, demonstrating the use of the [zio](https://github.com/lalinsky/zio) async I/O library.

## Features

Implements a subset of the Redis RESP2 (REdis Serialization Protocol) with the following commands:

- `PING` - Returns PONG
- `ECHO <message>` - Returns the message
- `SET <key> <value> [EX seconds|PX milliseconds]` - Stores a key-value pair with optional expiration
- `GET <key>` - Retrieves a value by key
- `DEL <key>` - Deletes a key
- `EXISTS <key>` - Checks if a key exists
- `TTL <key>` - Get remaining time to live in seconds (-1 if no expiration, -2 if key doesn't exist)
- `PTTL <key>` - Get remaining time to live in milliseconds (-1 if no expiration, -2 if key doesn't exist)
- `EXPIRE <key> <seconds>` - Set expiration on an existing key
- `PEXPIRE <key> <milliseconds>` - Set expiration on an existing key in milliseconds
- `PERSIST <key>` - Remove expiration from a key
- `SETEX <key> <seconds> <value>` - Set key with expiration in seconds
- `PSETEX <key> <milliseconds> <value>` - Set key with expiration in milliseconds

## Requirements

- Zig 0.15.1 or later

## Building

```bash
zig build
```

## Running

```bash
./zig-out/bin/mini-redis
```

The server will listen on `127.0.0.1:6379`.

## Testing

Use the official Redis CLI to test:

```bash
redis-cli -p 6379
```

Example session:

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET greeting "Hello, World!"
OK
127.0.0.1:6379> GET greeting
"Hello, World!"
127.0.0.1:6379> EXISTS greeting
(integer) 1
127.0.0.1:6379> DEL greeting
(integer) 1
127.0.0.1:6379> GET greeting
```
