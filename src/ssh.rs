use futures_util::FutureExt;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

#[derive(Clone, Default)]
pub(crate) struct SshConnector {
    pool: Arc<Mutex<HashMap<http::uri::Authority, Weak<openssh::Session>>>>,
}

pub(crate) struct SshStream {
    _child: openssh::Child<Arc<openssh::Session>>,
    stdin: TokioIo<openssh::ChildStdin>,
    stdout: TokioIo<openssh::ChildStdout>,
}

impl tower_service::Service<hyper::Uri> for SshConnector {
    type Response = SshStream;
    type Error = openssh::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, destination: hyper::Uri) -> Self::Future {
        let pool = self.pool.clone();
        async move {
            let authority = match destination.scheme() {
                Some(scheme) if scheme == "ssh" => destination.authority().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "Missing authority")
                }),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid scheme {:?}", destination.scheme()),
                )),
            }
            .map_err(openssh::Error::Connect)?;

            let session = {
                let mut pool = pool.lock().unwrap();
                // garbage collection
                pool.retain(|_, session| session.strong_count() > 0);
                pool.get(authority).and_then(Weak::upgrade)
            };

            // check if the session is alive
            let session = if let Some(session) = session {
                session.check().await.is_ok().then_some(session)
            } else {
                None
            };

            let session = if let Some(session) = session {
                session
            } else {
                let builder = openssh::SessionBuilder::default();
                let (builder, destination) = builder.resolve(authority.as_str());
                let tempdir = builder.launch_master(destination).await?;
                let session = Arc::new(openssh::Session::new_process_mux(tempdir));
                pool.lock()
                    .unwrap()
                    .insert(authority.clone(), Arc::downgrade(&session));
                session
            };

            let mut child = session
                .arc_command("docker")
                .arg("system")
                .arg("dial-stdio")
                .stdin(openssh::Stdio::piped())
                .stdout(openssh::Stdio::piped())
                .spawn()
                .await?;

            Ok(SshStream {
                stdin: TokioIo::new(child.stdin().take().unwrap()),
                stdout: TokioIo::new(child.stdout().take().unwrap()),
                _child: child,
            })
        }
        .boxed()
    }
}

impl SshStream {
    fn stdin(self: Pin<&mut Self>) -> Pin<&mut TokioIo<openssh::ChildStdin>> {
        Pin::new(&mut self.get_mut().stdin)
    }

    fn stdout(self: Pin<&mut Self>) -> Pin<&mut TokioIo<openssh::ChildStdout>> {
        Pin::new(&mut self.get_mut().stdout)
    }
}

impl hyper::rt::Read for SshStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        self.stdout().poll_read(cx, buf)
    }
}

impl hyper::rt::Write for SshStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stdin().poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.stdin().poll_write_vectored(cx, bufs)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stdin().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stdin().poll_shutdown(cx)
    }
}

impl hyper_util::client::legacy::connect::Connection for SshStream {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}
