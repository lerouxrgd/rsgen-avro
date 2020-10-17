pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Schema error: {}", .0)]
    Schema(String),
    #[error("Templating error: {}", .0)]
    Template(String),
    #[error("Unexpected io error: {}", .0)]
    Io(#[from] std::io::Error),
    #[error("AvroRs error: {}", .0)]
    AvroRs(#[from] avro_rs::Error),
}

impl From<tera::Error> for Error {
    fn from(source: tera::Error) -> Self {
        Error::Template(source.to_string())
    }
}

impl From<uuid::Error> for Error {
    fn from(source: uuid::Error) -> Self {
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
