use iroh_blobs::BlobsProtocol;

#[derive(Debug, Clone)]
pub struct Fileshare {
    blobs: BlobsProtocol,
}

impl Fileshare {
    pub const ALPN: &[u8] = b"/airdroppe/fileshare/1.0.0";

    pub fn new(blobs: BlobsProtocol) -> Self {
        Self { blobs }
    }
}
