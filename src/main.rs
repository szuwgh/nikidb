mod datafile;
mod db;
use db::DB;
fn main() {
    let d = DB::open("./dbfile");
    println!("hello world");
}
