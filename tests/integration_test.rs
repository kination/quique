use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use toqueue::cluster::{Cluster, Node};
use toqueue::protocol::*;
use toqueue::queue::Registry;
use toqueue::server::Server;

async fn start_test_server(port: u16) -> tokio::task::JoinHandle<()> {
    let addr = format!("127.0.0.1:{}", port);
    let nodes = vec![Node {
        id: "test-node".to_string(),
        addr: addr.clone(),
    }];
    let cluster = Cluster::new("test-node".to_string(), nodes).unwrap();

    tokio::spawn(async move {
        let server = Server::new(addr, "./test_data".to_string(), cluster);
        let _ = server.run().await;
    })
}

async fn connect_client(port: u16) -> TcpStream {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..10 {
        if let Ok(stream) = TcpStream::connect(&addr).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Failed to connect to test server");
}

async fn send_request(
    stream: &mut TcpStream,
    op: Op,
    body: &[u8],
) -> Result<(Status, Vec<u8>), Box<dyn std::error::Error>> {
    let header = Header {
        magic: MAGIC,
        version: VERSION,
        op,
        flags: 0,
        stream_id: 1,
        body_len: body.len() as u32,
    };

    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.extend_from_slice(body);
    stream.write_all(&buf).await?;

    let mut hdr_buf = [0u8; 16];
    stream.read_exact(&mut hdr_buf).await?;
    let body_len = u32::from_be_bytes([hdr_buf[12], hdr_buf[13], hdr_buf[14], hdr_buf[15]]) as usize;

    let mut response_body = vec![0u8; body_len];
    stream.read_exact(&mut response_body).await?;

    let status = Status::from(u16::from_be_bytes([response_body[0], response_body[1]]));
    Ok((status, response_body[2..].to_vec()))
}

#[tokio::test]
async fn test_create_topic() {
    let port = 7101;
    let _server = start_test_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = connect_client(port).await;

    let mut body = BytesMut::new();
    put_str(&mut body, "test-topic");

    let (status, _) = send_request(&mut client, Op::CreateTopic, &body).await.unwrap();
    assert_eq!(status, Status::Ok);
}

#[tokio::test]
async fn test_produce_and_consume() {
    let port = 7102;
    let _server = start_test_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = connect_client(port).await;

    // Produce
    let mut body = BytesMut::new();
    put_str(&mut body, "messages");
    put_bytes(&mut body, b"hello world");

    let (status, _) = send_request(&mut client, Op::Produce, &body).await.unwrap();
    assert_eq!(status, Status::Ok);

    // Consume
    let mut body = BytesMut::new();
    put_str(&mut body, "messages");
    put_u32(&mut body, 0); // Non-blocking

    let (status, payload) = send_request(&mut client, Op::Consume, &body).await.unwrap();
    assert_eq!(status, Status::Ok);

    let mut slice = &payload[..];
    let data = get_bytes(&mut slice).unwrap();
    assert_eq!(data, b"hello world");
}

#[tokio::test]
async fn test_consume_empty_queue() {
    let port = 7103;
    let _server = start_test_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = connect_client(port).await;

    let mut body = BytesMut::new();
    put_str(&mut body, "empty-queue");
    put_u32(&mut body, 0);

    let (status, _) = send_request(&mut client, Op::Consume, &body).await.unwrap();
    assert_eq!(status, Status::Empty);
}

#[tokio::test]
async fn test_blocking_consume() {
    let port = 7104;
    let _server = start_test_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut consumer = connect_client(port).await;
    let mut producer = connect_client(port).await;

    let consumer_task = tokio::spawn(async move {
        let mut body = BytesMut::new();
        put_str(&mut body, "blocking-queue");
        put_u32(&mut body, 3000); // 3 second timeout

        let (status, payload) = send_request(&mut consumer, Op::Consume, &body).await.unwrap();
        (status, payload)
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut body = BytesMut::new();
    put_str(&mut body, "blocking-queue");
    put_bytes(&mut body, b"delivered message");
    send_request(&mut producer, Op::Produce, &body).await.unwrap();

    let result = timeout(Duration::from_secs(5), consumer_task).await;
    assert!(result.is_ok());

    let (status, payload) = result.unwrap().unwrap();
    assert_eq!(status, Status::Ok);

    let mut slice = &payload[..];
    let data = get_bytes(&mut slice).unwrap();
    assert_eq!(data, b"delivered message");
}

#[tokio::test]
async fn test_fan_out() {
    let port = 7105;
    let _server = start_test_server(port).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = connect_client(port).await;

    // Create topic
    let mut body = BytesMut::new();
    put_str(&mut body, "fan-topic");
    send_request(&mut client, Op::CreateTopic, &body).await.unwrap();

    // Create queues
    let mut body = BytesMut::new();
    put_str(&mut body, "queue1");
    put_u32(&mut body, 1024);
    send_request(&mut client, Op::CreateQueue, &body).await.unwrap();

    let mut body = BytesMut::new();
    put_str(&mut body, "queue2");
    put_u32(&mut body, 1024);
    send_request(&mut client, Op::CreateQueue, &body).await.unwrap();

    // Bind queues to topic
    let mut body = BytesMut::new();
    put_str(&mut body, "fan-topic");
    put_str(&mut body, "queue1");
    send_request(&mut client, Op::BindQueue, &body).await.unwrap();

    let mut body = BytesMut::new();
    put_str(&mut body, "fan-topic");
    put_str(&mut body, "queue2");
    send_request(&mut client, Op::BindQueue, &body).await.unwrap();

    // Produce one message
    let mut body = BytesMut::new();
    put_str(&mut body, "fan-topic");
    put_bytes(&mut body, b"broadcast");
    send_request(&mut client, Op::Produce, &body).await.unwrap();

    // Consume from both queues
    let mut body = BytesMut::new();
    put_str(&mut body, "queue1");
    put_u32(&mut body, 0);
    let (status1, payload1) = send_request(&mut client, Op::Consume, &body).await.unwrap();
    assert_eq!(status1, Status::Ok);

    let mut body = BytesMut::new();
    put_str(&mut body, "queue2");
    put_u32(&mut body, 0);
    let (status2, payload2) = send_request(&mut client, Op::Consume, &body).await.unwrap();
    assert_eq!(status2, Status::Ok);

    let mut slice = &payload1[..];
    let data1 = get_bytes(&mut slice).unwrap();
    let mut slice = &payload2[..];
    let data2 = get_bytes(&mut slice).unwrap();

    assert_eq!(data1, b"broadcast");
    assert_eq!(data2, b"broadcast");
}
