use nikidb::db::DB;
use nikidb::option::Options;
fn main() {
    let c = Options::default();
    let mut d = DB::open("./dbfile", c).unwrap();
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
