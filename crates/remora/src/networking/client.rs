// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, net::SocketAddr, time::Duration};

use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
    time::sleep,
};

use crate::networking::worker::ConnectionWorker;

/// A client ran by the proxy (and the load generator) to connects to the server on the
/// primary machine and receive messages to process. The client can also leverage the
/// bidirectional connection to send messages back to the server.
pub struct NetworkClient<I, O> {
    /// The address of the validator.
    server_address: SocketAddr,
    /// The sender for messages received from the network to send to the application layer.
    tx_incoming: Sender<I>,
    /// The receiver for messages to send through the network.
    rx_outgoing: Receiver<O>,
}

impl<I, O> NetworkClient<I, O>
where
    I: Send + DeserializeOwned,
    O: Send + Serialize,
{
    /// Create a new client.
    pub fn new(
        server_address: SocketAddr,
        tx_incoming: Sender<I>,
        rx_outgoing: Receiver<O>,
    ) -> Self {
        Self {
            server_address,
            tx_incoming,
            rx_outgoing,
        }
    }

    /// Run the client.
    pub async fn run(self) -> io::Result<()> {
        tracing::info!("Trying to connect to server {}", self.server_address);

        let stream = loop {
            let socket = if self.server_address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };

            match socket.connect(self.server_address).await {
                Ok(stream) => break stream,
                Err(e) => {
                    tracing::info!("Failed to connect to server (retrying): {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        };
        tracing::info!("Connected to {}", self.server_address);

        let worker = ConnectionWorker::new(stream, self.tx_incoming, self.rx_outgoing);
        worker.run().await;
        Ok(())
    }

    /// Spawn the client in a new task.
    pub fn spawn(self) -> JoinHandle<io::Result<()>>
    where
        I: 'static,
        O: 'static,
    {
        tokio::spawn(async move { self.run().await })
    }

    /// Attempt to connect to the server.
    /// This returns the connection stream if successful, or an error if the connection fails.
    pub async fn connect(&self) -> io::Result<TcpStream> {
        tracing::info!("Trying to connect to server {}", self.server_address);

        let stream = loop {
            let socket: TcpSocket = if self.server_address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };

            match socket.connect(self.server_address).await {
                Ok(stream) => break stream,
                Err(e) => {
                    tracing::info!("Failed to connect to server (retrying): {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        };

        tracing::info!("Connected to {}", self.server_address);
        Ok(stream)
    }

    pub fn spawn_after_connect(self, stream: TcpStream) -> JoinHandle<io::Result<()>>
    where
        I: 'static,
        O: 'static,
    {
        tokio::spawn(async move {
            let worker = ConnectionWorker::new(stream, self.tx_incoming, self.rx_outgoing);
            worker.run().await;
            Ok(())
        })
    }
}
