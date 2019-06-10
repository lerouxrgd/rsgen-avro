pub type Result<T> = std::result::Result<T, RsgenError>;

#[derive(Debug, PartialEq)]
pub enum RsgenError {
    Schema(String),
    Template(String),
    Io(String),
    Failure(String),
}

impl std::fmt::Display for RsgenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RsgenError::{:?}", self)
    }
}

impl From<std::io::Error> for RsgenError {
    fn from(source: std::io::Error) -> Self {
        RsgenError::Io(source.to_string())
    }
}

impl From<failure::Error> for RsgenError {
    fn from(source: failure::Error) -> Self {
        RsgenError::Failure(source.to_string())
    }
}

impl From<tera::Error> for RsgenError {
    fn from(source: tera::Error) -> Self {
        RsgenError::Template(source.description().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display() {
        assert_eq!(
            r#"RsgenError::Schema("Some message")"#,
            RsgenError::Schema("Some message".into()).to_string()
        );
    }
}
