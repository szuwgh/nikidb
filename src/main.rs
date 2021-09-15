mod datafile;
mod db;
use db::DB;
fn main() {
    let d = DB::open();
    println!("hello world");
}
