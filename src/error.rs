use thiserror::Error;
use crate::compose::DependencyResolutionError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Schema error: {}", .0)]
    Schema(String),
    #[error("Templating error: {}", .0)]
    Template(String),
    #[error("Unexpected io error: {}", .0)]
    Io(#[from] std::io::Error),
    #[error("Avro failure: {}", .0)]
    Failure(String),
}

impl From<failure::Error> for Error {
    fn from(source: failure::Error) -> Self {
        Error::Failure(source.to_string())
    }
}

impl From<DependencyResolutionError> for Error {
    fn from(source: DependencyResolutionError) -> Self{ Error::Schema(source.msg)}
}

impl From<tera::Error> for Error {
    fn from(source: tera::Error) -> Self {
        Error::Template(source.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display() {
        assert_eq!(
            "Schema error: Some message",
            Error::Schema("Some message".into()).to_string()
        );
    }
}
