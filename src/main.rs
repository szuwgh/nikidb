mod datafile;
mod db;
mod error;
use db::DB;
fn main() {
    let d = DB::open("./dbfile");
    println!("hello world");
}
