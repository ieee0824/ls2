//! S3 flexible checksums (CRC32 / CRC32C / CRC64NVME / SHA1 / SHA256).
use crate::Error;
use base64::{Engine, engine::general_purpose::STANDARD};
use crc::{CRC_32_ISCSI, CRC_32_ISO_HDLC, CRC_64_NVME, Crc, Table};
use md5::Md5;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest, Sha256};

const CRC32: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISO_HDLC);
const CRC32C: Crc<u32, Table<16>> = Crc::<u32, Table<16>>::new(&CRC_32_ISCSI);
const CRC64NVME: Crc<u64, Table<16>> = Crc::<u64, Table<16>>::new(&CRC_64_NVME);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithm {
    pub const ALL: [Self; 5] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
    ];

    /// Name used by `x-amz-checksum-algorithm` and `<ChecksumAlgorithm>`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|a| a.name().eq_ignore_ascii_case(s))
    }
    /// Header carrying the value, e.g. `x-amz-checksum-crc32`.
    pub fn header(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64Nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }
    /// XML element carrying the value, e.g. `ChecksumCRC32`.
    pub fn xml_tag(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Crc64Nvme => "ChecksumCRC64NVME",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
        }
    }
    /// Base64 of the big-endian digest, as S3 transmits it.
    pub fn compute(self, data: &[u8]) -> String {
        match self {
            Self::Crc32 => STANDARD.encode(CRC32.checksum(data).to_be_bytes()),
            Self::Crc32c => STANDARD.encode(CRC32C.checksum(data).to_be_bytes()),
            Self::Crc64Nvme => STANDARD.encode(CRC64NVME.checksum(data).to_be_bytes()),
            Self::Sha1 => STANDARD.encode(Sha1::digest(data)),
            Self::Sha256 => STANDARD.encode(Sha256::digest(data)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    pub algorithm: ChecksumAlgorithm,
    /// Base64 digest.
    pub value: String,
}

impl Checksum {
    pub fn compute(algorithm: ChecksumAlgorithm, data: &[u8]) -> Self {
        Self {
            algorithm,
            value: algorithm.compute(data),
        }
    }
}

/// What the client asked for regarding the payload checksum.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ChecksumInput {
    /// No checksum supplied; the operation's default applies.
    #[default]
    Default,
    /// `x-amz-sdk-checksum-algorithm` without a value: compute and store.
    Compute(ChecksumAlgorithm),
    /// A value to verify before anything is stored.
    Verify(Checksum),
}

impl ChecksumInput {
    pub fn algorithm(&self) -> Option<ChecksumAlgorithm> {
        match self {
            Self::Default => None,
            Self::Compute(a) => Some(*a),
            Self::Verify(c) => Some(c.algorithm),
        }
    }
    /// Verifies a supplied value and returns the checksum to store.
    pub fn resolve(
        &self,
        data: &[u8],
        default: Option<ChecksumAlgorithm>,
    ) -> Result<Option<Checksum>, Error> {
        match self {
            Self::Default => Ok(default.map(|a| Checksum::compute(a, data))),
            Self::Compute(a) => Ok(Some(Checksum::compute(*a, data))),
            Self::Verify(expected) => {
                if expected.algorithm.compute(data) != expected.value {
                    return Err(Error::BadDigest(format!(
                        "The {} you specified did not match the calculated checksum.",
                        expected.algorithm.name()
                    )));
                }
                Ok(Some(expected.clone()))
            }
        }
    }
}

pub(crate) fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Rejects a body whose Content-MD5 does not match.
pub fn verify_md5(data: &[u8], expected: Option<[u8; 16]>) -> Result<[u8; 16], Error> {
    let actual = md5(data);
    if expected.is_some_and(|e| e != actual) {
        return Err(Error::BadDigest(
            "The Content-MD5 you specified did not match what we received.".into(),
        ));
    }
    Ok(actual)
}
