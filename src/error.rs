pub enum Error {
    IoFs { err: std::io::Error },
}

pub type ResultError<T> = Result<T, Error>;
