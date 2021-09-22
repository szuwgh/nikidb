mod datafile;
mod db;
mod util;
use db::DB;
fn main() {
    let d = DB::open("./dbfile");
    println!("hello world");
}
