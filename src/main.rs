mod datafile;
mod db;
mod error;
mod util;
use db::DB;
fn main() {
    let d = DB::open("./dbfile");
    println!("hello world");
}
