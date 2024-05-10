struct Quadifier;

impl Quadifier {
    const DIGIT_MASK: u32 = !((!0) << 5);
    const ALPHABET: &'static [u8; 32] = b"23456789abcdefghijkmnpqrstuvwxyz";

    pub fn quadify(n: u32, cs: &mut [u8]) {
        cs[0] = Self::ALPHABET[((n >> 15) & Self::DIGIT_MASK) as usize];
        cs[1] = Self::ALPHABET[((n >> 10) & Self::DIGIT_MASK) as usize];
        cs[2] = Self::ALPHABET[((n >> 5) & Self::DIGIT_MASK) as usize];
        cs[3] = Self::ALPHABET[(n & Self::DIGIT_MASK) as usize];
    }
}

pub struct LongFormatter;

impl LongFormatter {
    const PUNCT: &'static [u8;4] = b"-._~";

    pub fn format(x: u64) -> [u8; 14] {
        let mut result = [0; 14];
        Quadifier::quadify(x as u32, &mut result[0..]);
        result[4] = Self::PUNCT[(x >> 60) as usize & 3];
        Quadifier::quadify((x >> 20) as u32, &mut result[5..]);
        result[9] = Self::PUNCT[(x >> 62) as usize & 3];
        Quadifier::quadify((x >> 40) as u32, &mut result[10..]);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agrees_with_reference_implementation() {
        assert_eq!(&LongFormatter::format(0x1234567890abcdef), b"rmhh.h4ac-6f4q");
    }
}
