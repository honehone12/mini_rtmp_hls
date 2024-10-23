use anyhow::anyhow;
use rtmp::rtmp::RtmpServer;
use hls::remuxer::HlsRemuxer;
use streamhub::{
    define::{BroadcastEventReceiver, StreamHubEventSender}, 
    StreamsHub
};

struct RtmpConfig {
    port: u16
}

struct HlsConfig {
    port: u16
}

struct Service {
    rtmp_cfg: RtmpConfig,
    hls_cfg: HlsConfig
}

impl Service {
    fn new(rtmp_port: u16, hls_port: u16) -> Self {
        Self {
            rtmp_cfg: RtmpConfig { 
                port: rtmp_port 
            },
            hls_cfg: HlsConfig { 
                port: hls_port 
            }
        }
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut stream_hub = StreamsHub::new(None);

        let rtmp_future = self.run_rtmp(stream_hub.get_hub_event_sender());
        let remuxer_future = self.run_remuxer(
            stream_hub.get_hub_event_sender(), 
            stream_hub.get_client_event_consumer(), 
            true
        );
        let hls_future = self.run_hls();
        stream_hub.set_hls_enabled(true); 
        let stream_hub_future = self.run_stream_hub(stream_hub);

        match tokio::try_join!(
            rtmp_future,
            remuxer_future,
            hls_future,
            stream_hub_future
        ) {
            Ok(_) => Ok(()),
            Err(e) => Err(e)
        }
    }

    async fn run_stream_hub(&self, mut stream_hub: StreamsHub) 
    -> anyhow::Result<()> {
        stream_hub.run().await;
        Ok(())
    }

    async fn run_rtmp(&self, stream_event_sender: StreamHubEventSender)
    -> anyhow::Result<()> {
        let mut rtmp_server = RtmpServer::new(
            format!("127.0.0.1:{}", self.rtmp_cfg.port), 
            stream_event_sender, 
            1, 
            None
        );
        rtmp_server.run().await?;
        Ok(())
    }

    async fn run_remuxer(
        &self,
        stream_event_sender: StreamHubEventSender,
        broadcast_event_receiver: BroadcastEventReceiver,
        need_record: bool
    ) -> anyhow::Result<()> {
        let mut remuxer = HlsRemuxer::new(
            broadcast_event_receiver, 
            stream_event_sender, 
            need_record
        );
        remuxer.run().await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }

    async fn run_hls(&self) -> anyhow::Result<()> {
        hls::server::run(
            format!("127.0.0.1:{}", self.hls_cfg.port), 
            None
        ).await
            .map_err(|e| anyhow!("{e}"))?;
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let service = Service::new(1935, 8080);
    if let Err(e) = service.run().await {
        panic!("{e}");
    }
}
