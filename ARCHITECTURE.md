# quique?

Project 'quique' is to build high-performance in-memory messaging queue system written in Rust. This document explain its architecture, leader election mechanism, and communication protocol.

__Architectures described below is still on-going status__

## 1. Core Architecture

'quique' works on asynchronous server/client model running based on TCP. It listens for client connections on specified instance (or broker?), and each client connection is handled by separated async task. This allows efficient concurrent processing of multiple client requests.

### 1.1. Distributed Leader Election & Routing

This system is designed as decentralized system without central master node (like ZooKeeper). Instead, it uses **rendezvous hashing** to determine 'leader node' for each topic.

The `leader_of` function is central to this mechanism:

*   **Decentralized Consensus**: Every nodes in cluster can independently calculate which node is responsible for a given topic, using same hashing algorithm. As long as the node list (`QBUS_NODES`) is consistent across the cluster, all nodes will reach to same conclusion without extra communication.
*   **Rendezvous Hashing**: The leader is selected by calculating `hash(node_id + topic)` for all nodes and choosing the one with the highest score. This ensures an even distribution of topics across the cluster (Load Balancing).
*   **Routing & Redirection**: When a node receives a request (e.g., `Produce`, `Consume`), it checks if it is the leader for the requested topic.
    *   If leader, it processes the request.
    *   If not, it responds with a `Redirect` status containing the address of the actual leader. The client then reconnects to the correct node.

## 2. Communication Protocol

All communication between the client and server is done via a custom binary protocol. Every message consists of a **Header** and a **Body**.

```
+------------------+----------------------+
|  Header (16 bytes) |  Body (Variable Len) |
+------------------+----------------------+
```

### 2.1. Header

The header is 'fixed-size 16-byte block' which locates at the beginning of every message, containing essential metadata.

| Field | Size (Bytes) | Description | Example |
| :--- | :--- | :--- | :--- |
| `magic` | 4 | **Protocol Identifier.** A unique value to verify that the message follows the Quique protocol. | `0x51425553` ('QBUS'). If the first 4 bytes do not match, the connection is considered invalid. |
| `version` | 1 | **Protocol Version.** Allows for future protocol changes and backward compatibility. | `1`. Servers can reject or handle older clients based on this version. |
| `op` | 1 | **Operation Code.** Indicates the type of request or response (e.g., `Produce`, `Consume`). | `Op::Produce` (e.g., `0x02`). Tells the server to execute the message publication logic. |
| `flags` | 2 | **Flags.** Reserved for additional attributes like compression or priority. | Currently `0`. Can be used for bitwise flags in the future. |
| `stream_id` | 4 | **Request/Response ID.** Used to match responses to requests in an asynchronous environment. | If a client sends a request with ID `101`, the server responds with ID `101`. |
| `body_len` | 4 | **Body Length.** The length of the following body data in bytes. | If the body is 115 bytes, the server reads exactly 115 more bytes after the header. |

### 2.2. Body

Body contains actual payload and its structure depends on the `op` code in the header.

**Example `Produce` Request Body:**
`[Topic Name (str)] [Message Content (bytes)]`

*   **`str`**: `[Length (u16)]` + `[UTF-8 String bytes]`
*   **`bytes`**: `[Length (u32)]` + `[Byte Array]`

## 3. Data Transmission Flow (Example: Produce)

The process of a client publishing a message (`Produce`) illustrates the interaction between the protocol and the architecture:

1.  **Client: Request Creation**
    *   The client constructs the Body containing the topic and message content.
    *   It creates the Header with `op` set to `Produce` and the calculated body length.
    *   Sends `[Header][Body]` to the server.

2.  **Server: Reception & Processing**
    *   The server reads the 16-byte Header.
    *   It waits until `body_len` bytes of Body data are fully received.
    *   It parses the Body to extract the topic and message.
    *   **Leader Check**: The server calls `leader_of(topic)` to check if it is the leader.
        *   **If Leader**: It writes the message to the `DiskLog` (disk) and adds it to the memory queue.
        *   **If Not Leader**: It sends a `Redirect` response with the address of the actual leader.

3.  **Server: Response**
    *   The server constructs a response Body (e.g., `Status::Ok` or `Status::Redirect`).
    *   Sends `[Header][Body]` back to the client.

4.  **Client: Response Handling**
    *   The client receives the response.
    *   If the status is `Redirect`, the client connects to the new address and retries the request.
