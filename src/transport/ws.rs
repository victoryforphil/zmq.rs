use super::AcceptStopHandle;
use crate::async_rt;
use crate::codec::FramedIo;
use crate::endpoint::{Endpoint, Host, Port};
use crate::task_handle::TaskHandle;
use crate::ZmqResult;

use async_tungstenite::tungstenite::Message;
use futures::{select, FutureExt, SinkExt, StreamExt};

// Common channel-based wrapper structure
struct WebSocketChannelWrapper {
    read_channel: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
    write_channel: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    read_buffer: Vec<u8>,
    read_pos: usize,
}

impl WebSocketChannelWrapper {
    fn new(
        read_channel: futures::channel::mpsc::UnboundedReceiver<Vec<u8>>,
        write_channel: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    ) -> Self {
        Self {
            read_channel,
            write_channel,
            read_buffer: Vec::new(),
            read_pos: 0,
        }
    }
}

#[cfg(feature = "tokio-runtime")]
pub(crate) async fn connect(host: &Host, port: Port, use_tls: bool) -> ZmqResult<(FramedIo, Endpoint)> {
    use tokio::net::TcpStream;
    
    let protocol = if use_tls { "wss" } else { "ws" };
    let url = format!("{}://{}:{}", protocol, host, port);
    
    let peer_endpoint = if use_tls {
        Endpoint::Wss(host.clone(), port)
    } else {
        Endpoint::Ws(host.clone(), port)
    };
    
    // Create async channels for communication
    let (tx_bytes, mut rx_bytes) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    let (tx_ws, rx_ws) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    
    if use_tls {
        #[cfg(feature = "wss-transport")]
        {
            let tcp_stream = TcpStream::connect((host.to_string().as_str(), port)).await?;
            let connector: tokio_native_tls::TlsConnector = 
                tokio_native_tls::native_tls::TlsConnector::builder()
                    .danger_accept_invalid_certs(true) // For self-signed certs - can be configurable
                    .build()
                    .map_err(|_e| crate::ZmqError::Other("TLS connector creation failed"))?
                    .into();
            
            let domain = match host {
                Host::Domain(d) => d.as_str(),
                _ => "localhost",
            };
            
            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(|_e| crate::ZmqError::Other("TLS connection failed"))?;
            
            let (ws_stream, _) = async_tungstenite::tokio::client_async(&url, tls_stream)
                .await
                .map_err(|_e| crate::ZmqError::Other("WebSocket connection failed"))?;
            
            // Create read and write halves that convert between WebSocket messages and raw bytes
            let (mut write, mut read) = ws_stream.split();
            
            // Spawn task to read WebSocket messages and convert to bytes
            async_rt::task::spawn(async move {
                while let Some(msg_result) = read.next().await {
                    if let Ok(Message::Binary(data)) = msg_result {
                        if tx_ws.unbounded_send(data).is_err() {
                            break;
                        }
                    } else if let Ok(Message::Close(_)) = msg_result {
                        break;
                    }
                }
            });
            
            // Spawn task to write bytes as WebSocket messages
            async_rt::task::spawn(async move {
                while let Some(data) = rx_bytes.next().await {
                    if write.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
            });
        }
        #[cfg(not(feature = "wss-transport"))]
        {
            return Err(crate::ZmqError::Other("WSS transport not enabled"));
        }
    } else {
        let tcp_stream = TcpStream::connect((host.to_string().as_str(), port)).await?;
        let (ws_stream, _) = async_tungstenite::tokio::client_async(&url, tcp_stream)
            .await
            .map_err(|_e| crate::ZmqError::Other("WebSocket connection failed"))?;
        
        // Create read and write halves that convert between WebSocket messages and raw bytes
        let (mut write, mut read) = ws_stream.split();
        
        // Spawn task to read WebSocket messages and convert to bytes
        async_rt::task::spawn(async move {
            while let Some(msg_result) = read.next().await {
                if let Ok(Message::Binary(data)) = msg_result {
                    if tx_ws.unbounded_send(data).is_err() {
                        break;
                    }
                } else if let Ok(Message::Close(_)) = msg_result {
                    break;
                }
            }
        });
        
        // Spawn task to write bytes as WebSocket messages
        async_rt::task::spawn(async move {
            while let Some(data) = rx_bytes.next().await {
                if write.send(Message::Binary(data)).await.is_err() {
                    break;
                }
            }
        });
    }
    
    // Create wrapper that implements AsyncRead/AsyncWrite using channels
    let wrapper = WebSocketChannelWrapper::new(rx_ws, tx_bytes);
    
    Ok((super::make_framed(wrapper), peer_endpoint))
}

#[cfg(any(feature = "async-std-runtime", feature = "async-dispatcher-runtime"))]
pub(crate) async fn connect(host: &Host, port: Port, use_tls: bool) -> ZmqResult<(FramedIo, Endpoint)> {
    use async_std::net::TcpStream;
    
    let protocol = if use_tls { "wss" } else { "ws" };
    let url = format!("{}://{}:{}", protocol, host, port);
    
    let peer_endpoint = if use_tls {
        Endpoint::Wss(host.clone(), port)
    } else {
        Endpoint::Ws(host.clone(), port)
    };
    
    // Create async channels for communication
    let (tx_bytes, mut rx_bytes) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    let (tx_ws, rx_ws) = futures::channel::mpsc::unbounded::<Vec<u8>>();
    
    if use_tls {
        #[cfg(feature = "wss-transport")]
        {
            use async_native_tls::TlsConnector;
            
            let tcp_stream = TcpStream::connect((host.to_string().as_str(), port)).await?;
            let connector = TlsConnector::new()
                .danger_accept_invalid_certs(true); // For self-signed certs - can be configurable
            
            let domain = match host {
                Host::Domain(d) => d.as_str(),
                _ => "localhost",
            };
            
            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(|_e| crate::ZmqError::Other("TLS connection failed"))?;
            
            let (ws_stream, _) = async_tungstenite::async_std::client_async(&url, tls_stream)
                .await
                .map_err(|_e| crate::ZmqError::Other("WebSocket connection failed"))?;
            
            // Create read and write halves that convert between WebSocket messages and raw bytes
            let (mut write, mut read) = ws_stream.split();
            
            // Spawn task to read WebSocket messages and convert to bytes
            async_rt::task::spawn(async move {
                while let Some(msg_result) = read.next().await {
                    if let Ok(Message::Binary(data)) = msg_result {
                        if tx_ws.unbounded_send(data).is_err() {
                            break;
                        }
                    } else if let Ok(Message::Close(_)) = msg_result {
                        break;
                    }
                }
            });
            
            // Spawn task to write bytes as WebSocket messages
            async_rt::task::spawn(async move {
                while let Some(data) = rx_bytes.next().await {
                    if write.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
            });
        }
        #[cfg(not(feature = "wss-transport"))]
        {
            return Err(crate::ZmqError::Other("WSS transport not enabled"));
        }
    } else {
        let tcp_stream = TcpStream::connect((host.to_string().as_str(), port)).await?;
        let (ws_stream, _) = async_tungstenite::async_std::client_async(&url, tcp_stream)
            .await
            .map_err(|_e| crate::ZmqError::Other("WebSocket connection failed"))?;
        
        // Create read and write halves that convert between WebSocket messages and raw bytes
        let (mut write, mut read) = ws_stream.split();
        
        // Spawn task to read WebSocket messages and convert to bytes
        async_rt::task::spawn(async move {
            while let Some(msg_result) = read.next().await {
                if let Ok(Message::Binary(data)) = msg_result {
                    if tx_ws.unbounded_send(data).is_err() {
                        break;
                    }
                } else if let Ok(Message::Close(_)) = msg_result {
                    break;
                }
            }
        });
        
        // Spawn task to write bytes as WebSocket messages
        async_rt::task::spawn(async move {
            while let Some(data) = rx_bytes.next().await {
                if write.send(Message::Binary(data)).await.is_err() {
                    break;
                }
            }
        });
    }
    
    // Create wrapper that implements AsyncRead/AsyncWrite using channels
    let wrapper = WebSocketChannelWrapper::new(rx_ws, tx_bytes);
    
    Ok((super::make_framed(wrapper), peer_endpoint))
}

#[cfg(feature = "tokio-runtime")]
pub(crate) async fn begin_accept<T>(
    host: Host,
    port: Port,
    use_tls: bool,
    cback: impl Fn(ZmqResult<(FramedIo, Endpoint)>) -> T + Send + 'static,
) -> ZmqResult<(Endpoint, AcceptStopHandle)>
where
    T: std::future::Future<Output = ()> + Send + 'static,
{
    use tokio::net::TcpListener;
    
    if use_tls {
        return Err(crate::ZmqError::Other(
            "WSS server support requires certificate configuration (not yet implemented)"
        ));
    }
    
    let listener = TcpListener::bind((host.to_string().as_str(), port)).await?;
    let resolved_addr = listener.local_addr()?;
    let resolved_endpoint = if use_tls {
        Endpoint::Wss(resolved_addr.ip().into(), resolved_addr.port())
    } else {
        Endpoint::Ws(resolved_addr.ip().into(), resolved_addr.port())
    };
    
    let (stop_channel, stop_callback) = futures::channel::oneshot::channel::<()>();
    
    let task_handle = async_rt::task::spawn(async move {
        let mut stop_callback = stop_callback.fuse();
        loop {
            select! {
                incoming = listener.accept().fuse() => {
                    let result = match incoming {
                        Ok((tcp_stream, remote_addr)) => {
                            match async_tungstenite::tokio::accept_async(tcp_stream).await {
                                Ok(ws_stream) => {
                                    let peer_endpoint = Endpoint::Ws(remote_addr.ip().into(), remote_addr.port());
                                    
                                    let (mut write, mut read) = ws_stream.split();
                                    
                                    let (tx_bytes, mut rx_bytes) = futures::channel::mpsc::unbounded::<Vec<u8>>();
                                    let (tx_ws, rx_ws) = futures::channel::mpsc::unbounded::<Vec<u8>>();
                                    
                                    async_rt::task::spawn(async move {
                                        while let Some(msg_result) = read.next().await {
                                            if let Ok(Message::Binary(data)) = msg_result {
                                                if tx_ws.unbounded_send(data).is_err() {
                                                    break;
                                                }
                                            } else if let Ok(Message::Close(_)) = msg_result {
                                                break;
                                            }
                                        }
                                    });
                                    
                                    async_rt::task::spawn(async move {
                                        while let Some(data) = rx_bytes.next().await {
                                            if write.send(Message::Binary(data)).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                    
                                    let wrapper = WebSocketChannelWrapper::new(rx_ws, tx_bytes);
                                    Ok((super::make_framed(wrapper), peer_endpoint))
                                }
                                Err(_e) => Err(crate::ZmqError::Other("WebSocket handshake failed")),
                            }
                        }
                        Err(e) => Err(e.into()),
                    };
                    cback(result).await;
                }
                _ = stop_callback => {
                    break;
                }
            }
        }
        Ok(())
    });
    
    Ok((
        resolved_endpoint,
        AcceptStopHandle(TaskHandle::new(stop_channel, task_handle)),
    ))
}

#[cfg(any(feature = "async-std-runtime", feature = "async-dispatcher-runtime"))]
pub(crate) async fn begin_accept<T>(
    host: Host,
    port: Port,
    use_tls: bool,
    cback: impl Fn(ZmqResult<(FramedIo, Endpoint)>) -> T + Send + 'static,
) -> ZmqResult<(Endpoint, AcceptStopHandle)>
where
    T: std::future::Future<Output = ()> + Send + 'static,
{
    use async_std::net::TcpListener;
    
    if use_tls {
        return Err(crate::ZmqError::Other(
            "WSS server support requires certificate configuration (not yet implemented)"
        ));
    }
    
    let listener = TcpListener::bind((host.to_string().as_str(), port)).await?;
    let resolved_addr = listener.local_addr()?;
    let resolved_endpoint = if use_tls {
        Endpoint::Wss(resolved_addr.ip().into(), resolved_addr.port())
    } else {
        Endpoint::Ws(resolved_addr.ip().into(), resolved_addr.port())
    };
    
    let (stop_channel, stop_callback) = futures::channel::oneshot::channel::<()>();
    
    let task_handle = async_rt::task::spawn(async move {
        let mut stop_callback = stop_callback.fuse();
        loop {
            select! {
                incoming = listener.accept().fuse() => {
                    let result = match incoming {
                        Ok((tcp_stream, remote_addr)) => {
                            match async_tungstenite::async_std::accept_async(tcp_stream).await {
                                Ok(ws_stream) => {
                                    let peer_endpoint = Endpoint::Ws(remote_addr.ip().into(), remote_addr.port());
                                    
                                    let (mut write, mut read) = ws_stream.split();
                                    
                                    let (tx_bytes, mut rx_bytes) = futures::channel::mpsc::unbounded::<Vec<u8>>();
                                    let (tx_ws, rx_ws) = futures::channel::mpsc::unbounded::<Vec<u8>>();
                                    
                                    async_rt::task::spawn(async move {
                                        while let Some(msg_result) = read.next().await {
                                            if let Ok(Message::Binary(data)) = msg_result {
                                                if tx_ws.unbounded_send(data).is_err() {
                                                    break;
                                                }
                                            } else if let Ok(Message::Close(_)) = msg_result {
                                                break;
                                            }
                                        }
                                    });
                                    
                                    async_rt::task::spawn(async move {
                                        while let Some(data) = rx_bytes.next().await {
                                            if write.send(Message::Binary(data)).await.is_err() {
                                                break;
                                            }
                                        }
                                    });
                                    
                                    let wrapper = WebSocketChannelWrapper::new(rx_ws, tx_bytes);
                                    Ok((super::make_framed(wrapper), peer_endpoint))
                                }
                                Err(_e) => Err(crate::ZmqError::Other("WebSocket handshake failed")),
                            }
                        }
                        Err(e) => Err(e.into()),
                    };
                    cback(result).await;
                }
                _ = stop_callback => {
                    break;
                }
            }
        }
        Ok(())
    });
    
    Ok((
        resolved_endpoint,
        AcceptStopHandle(TaskHandle::new(stop_channel, task_handle)),
    ))
}

// Tokio-specific AsyncRead/AsyncWrite implementation
#[cfg(feature = "tokio-runtime")]
impl tokio::io::AsyncRead for WebSocketChannelWrapper {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        
        // If we have data in the buffer, return it
        if self.read_pos < self.read_buffer.len() {
            let remaining = self.read_buffer.len() - self.read_pos;
            let to_copy = remaining.min(buf.remaining());
            buf.put_slice(&self.read_buffer[self.read_pos..self.read_pos + to_copy]);
            self.read_pos += to_copy;
            
            // Clear buffer if we've consumed it all
            if self.read_pos >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_pos = 0;
            }
            
            return Poll::Ready(Ok(()));
        }

        // Read next message from channel
        match self.read_channel.poll_next_unpin(cx) {
            Poll::Ready(Some(data)) => {
                let to_copy = data.len().min(buf.remaining());
                buf.put_slice(&data[..to_copy]);
                
                // If there's more data, store it in the buffer
                if to_copy < data.len() {
                    self.read_buffer = data[to_copy..].to_vec();
                    self.read_pos = 0;
                }
                
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tokio-runtime")]
impl tokio::io::AsyncWrite for WebSocketChannelWrapper {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.write_channel.unbounded_send(buf.to_vec()) {
            Ok(()) => std::task::Poll::Ready(Ok(buf.len())),
            Err(_) => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WebSocket write channel closed",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

// async-std-specific AsyncRead/AsyncWrite implementation
#[cfg(any(feature = "async-std-runtime", feature = "async-dispatcher-runtime"))]
impl futures::AsyncRead for WebSocketChannelWrapper {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use std::task::Poll;
        
        // If we have data in the buffer, return it
        if self.read_pos < self.read_buffer.len() {
            let remaining = self.read_buffer.len() - self.read_pos;
            let to_copy = remaining.min(buf.len());
            buf[..to_copy].copy_from_slice(&self.read_buffer[self.read_pos..self.read_pos + to_copy]);
            self.read_pos += to_copy;
            
            // Clear buffer if we've consumed it all
            if self.read_pos >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_pos = 0;
            }
            
            return Poll::Ready(Ok(to_copy));
        }

        // Read next message from channel
        match self.read_channel.poll_next_unpin(cx) {
            Poll::Ready(Some(data)) => {
                let to_copy = data.len().min(buf.len());
                buf[..to_copy].copy_from_slice(&data[..to_copy]);
                
                // If there's more data, store it in the buffer
                if to_copy < data.len() {
                    self.read_buffer = data[to_copy..].to_vec();
                    self.read_pos = 0;
                }
                
                Poll::Ready(Ok(to_copy))
            }
            Poll::Ready(None) => Poll::Ready(Ok(0)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(any(feature = "async-std-runtime", feature = "async-dispatcher-runtime"))]
impl futures::AsyncWrite for WebSocketChannelWrapper {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.write_channel.unbounded_send(buf.to_vec()) {
            Ok(()) => std::task::Poll::Ready(Ok(buf.len())),
            Err(_) => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WebSocket write channel closed",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}


