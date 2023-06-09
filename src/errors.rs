use thiserror::Error;

pub enum Errors {}
#[derive(Error, Debug)]

pub type Result<T> = std::result::Result<T, Errors>;