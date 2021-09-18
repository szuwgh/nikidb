pub const MSB: u8 = 0b1000_0000;
/// All bits except for the most significant. Can be used as bitmask to drop the most-signficant
/// bit using `&` (binary-and).
const DROP_MSB: u8 = 0b0111_1111;

#[inline]
fn zigzag_encode(from: i64) -> u64 {
    ((from << 1) ^ (from >> 63)) as u64
}

#[inline]
fn zigzag_decode(from: u64) -> i64 {
    ((from >> 1) ^ (-((from & 1) as i64)) as u64) as i64
}

pub fn put_varint64(dst: &mut [u8], n: i64) -> usize {
    let x: u64 = zigzag_encode(n as i64);
    put_varuint64(dst, x)
}

pub fn put_varuint64(dst: &mut [u8], n: u64) -> usize {
    let mut n = n;
    let mut i = 0;
    while n >= 0x80 {
        dst[i] = MSB | (n as u8);
        i += 1;
        n >>= 7;
    }
    dst[i] = n as u8;
    i + 1
}

pub fn read_varint64(src: &[u8]) -> Option<(i64, usize)> {
    if let Some((result, size)) = read_varuint64(src) {
        Some((zigzag_decode(result), size))
    } else {
        None
    }
}

pub fn read_varuint64(src: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut success = false;
    for b in src.iter() {
        let msb_dropped = b & DROP_MSB;
        result |= (msb_dropped as u64) << shift;
        shift += 7;
        if b & MSB == 0 || shift > (9 * 7) {
            success = b & MSB == 0;
            break;
        }
    }
    if success {
        Some((result, shift / 7 as usize))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put_varint64() {
        let mut buf: [u8; 10] = [0; 10];
        let sz = put_varuint64(&mut buf[..], 123456789);
        let v = read_varuint64(&buf[..]);
        match v {
            Some((num, i)) => println!("{},{}", num, i),
            None => println!("read node"),
        }

        let sz = put_varint64(&mut buf[..], -123456789);
        let v = read_varint64(&buf[..]);
        match v {
            Some((num, i)) => println!("{},{}", num, i),
            None => println!("read node"),
        }
    }
}
