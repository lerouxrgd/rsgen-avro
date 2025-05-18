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
    #[error("Avro error: {}", .0)]
    Avro(#[from] Box<apache_avro::Error>),
    #[error("Invalid glob pattern: {}", .0)]
    GlobPattern(#[from] glob::PatternError),
}

impl From<tera::Error> for Error {
    fn from(source: tera::Error) -> Self {
        Error::Template(source.to_string())
    }
}

impl From<apache_avro::Error> for Error {
    fn from(source: apache_avro::Error) -> Self {
        Error::Avro(source.into())
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
