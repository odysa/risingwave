// use anyhow::Result;
// use tokio::sync::oneshot::Sender;
// use tokio::task::JoinHandle;

// /// `TelemetryReportManager` manages telemetry reporting
// #[derive(Debug)]
// pub struct TelemetryReportManager {
//     shutdown_tx: Option<Sender<()>>,
//     handler: Option<JoinHandle<()>>,
// }

// impl TelemetryReportManager {
//     pub fn new() -> Self {
//         Self {
//             shutdown_tx: None,
//             handler: None,
//         }
//     }

//     // pub fn start(&mut self) -> Result<()> {
//     //     let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

//     //     // self.handler = Some(handler);
//     //     self.shutdown_tx = Some(shutdown_tx);
//     //     Ok(())
//     // }

//     // pub fn shutdown(&self) {
//     //     if let Some(ref mut shutdown_tx) = self.shutdown_tx {
//     //         shutdown_tx.send(());
//     //     }
//     // }
// }
