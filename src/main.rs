mod config;
mod datafile;
mod db;
mod error;
mod util;
use db::DB;
fn main() {
    let mut d = DB::open("./dbfile").unwrap();
    let offset = d
        .put("cb".as_bytes(), "aaabbbccccffffff".as_bytes())
        .unwrap();
    let e = d.read("a".as_bytes()).unwrap();
    println!(
        "value is {}",
        std::str::from_utf8(e.value.as_slice()).unwrap()
    );
    //println!("offset :{}", offset);
}
