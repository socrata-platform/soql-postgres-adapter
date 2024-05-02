CREATE OR REPLACE FUNCTION make_obfuscator(key BYTEA)
  RETURNS BYTEA
  LANGUAGE PLRUST
  IMMUTABLE STRICT PARALLEL SAFE COST 100
AS
$$
mod blowfish {
  undivert(src/blowfish.rs)
}
use blowfish::OwnedBlowfish;
let Ok(key) = <&[u8; blowfish::KEYSIZE]>::try_from(key) else {
    return Err("Invalid key".into());
};
Ok(Some(OwnedBlowfish::new(&key).into_bytes()))
$$;

CREATE OR REPLACE FUNCTION obfuscate(obfuscator BYTEA, value bigint)
  RETURNS BIGINT
  LANGUAGE PLRUST
  IMMUTABLE STRICT PARALLEL SAFE COST 1
AS
$$
mod blowfish {
  undivert(src/blowfish.rs)
}
use blowfish::{BorrowedBlowfish, Blowfish};
let Ok(obfuscator) = <&[u8; blowfish::BYTESIZE]>::try_from(obfuscator) else {
    return Err("Invalid obfuscator".into());
};
Ok(Some(BorrowedBlowfish::from_bytes(obfuscator)?.encrypt(value as u64) as i64))
$$;

CREATE OR REPLACE FUNCTION make_rowid(value BIGINT)
  RETURNS TEXT
  LANGUAGE PLRUST
  IMMUTABLE STRICT PARALLEL SAFE COST 1
AS
$$
mod longformatter {
  undivert(src/longformatter.rs)
}
let mut result = String::with_capacity(4 + 14);
result.push_str("row-");
result.push_str(std::str::from_utf8(&longformatter::LongFormatter::`format'(value as u64)).unwrap());
Ok(Some(result.to_string()))
$$;
