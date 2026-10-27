use super::Fileshare;
use iroh::protocol::ProtocolHandler;

impl ProtocolHandler for Fileshare {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        Ok(())
    }
}
