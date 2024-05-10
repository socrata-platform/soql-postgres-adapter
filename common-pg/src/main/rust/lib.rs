pub mod blowfish;
pub mod longformatter;

// Exists so that the relevant things get monomorphized for inspection
// with `cargo asm common_pg::encrypt --rust`
use blowfish::{BorrowedBlowfish, Blowfish};
pub fn encrypt(bf: &BorrowedBlowfish, value: u64) -> u64 {
    bf.encrypt(value)
}
