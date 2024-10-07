# ElixirCache

ElixirCache is a high-performance, in-memory data store implemented in Elixir. It's designed to provide fast data access and storage capabilities, similar to Redis, with additional features for replication and WebSocket-based interaction.

## Features

- In-memory key-value storage
- Support for basic Redis-like commands (SET, GET, INCR, etc.)
- Master-Slave replication
- Stream data structure support (XADD, XRANGE, XREAD)
- WebSocket interface for real-time interaction
- Transaction support (MULTI, EXEC, DISCARD)

## Getting Started

### Prerequisites

- Elixir 1.12 or later
- Erlang/OTP 23 or later

### Installation and Running with Docker

1. Clone the repository:
   ```
   git clone https://github.com/ProgMastermind/Elixir_Backend.git
   cd Elixir_Backend
   ```

2. Build and run the Docker container:
   ```
   docker-compose up --build
   ```

This will start the ElixirCache server, accessible on port 3001.

### Manual Installation (without Docker)

If you prefer to run ElixirCache without Docker:
1. Clone the repository:
   ```
   git clone https://github.com/ProgMastermind/Elixir_Backend.git
   cd Elixir_Backend
   ```

2. Install dependencies:
   ```
   mix deps.get
   ```

3. Compile the project:
   ```
   mix compile
   ```

### Running the Server

To start the ElixirCache server:

```
mix run --no-halt
```

By default, the server will start on port 6379 for Redis protocol connections and port 3001 for WebSocket connections.

## Usage

### WebSocket Interface

ElixirCache provides a WebSocket interface for real-time interactions. You can connect to:

- `ws://localhost:3001/ws/master` for master connection
- `ws://localhost:3001/ws/slave` for slave connection

Send JSON messages in the format:

```json
{
  "type": "COMMAND",
  "command": "SET mykey myvalue"
}
```

### Replication

ElixirCache supports master-slave replication. To start a slave instance:

1. The server will automatically start a slave when a WebSocket connection is made to the slave endpoint.
2. The slave will connect to the master (default: localhost:6379) and sync data.

## Key Features

### Streams

ElixirCache implements Redis-like streams with commands such as XADD, XRANGE, and XREAD.

### Transactions

Use MULTI to start a transaction, EXEC to execute it, and DISCARD to cancel.

## Development

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Inspired by Redis
- Built with Elixir and love for high-performance data stores

## Contact

- GitHub: [ProgMastermind](https://github.com/ProgMastermind)
- LinkedIn: [Prakash Collymorey](https://www.linkedin.com/in/soulglory/)
- Twitter: [@PrakashCollymo1](https://x.com/PrakashCollymo1)
