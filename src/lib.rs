#[macro_use] extern crate log;
#[macro_use] extern crate failure;

pub mod client;
pub mod protobuf;

pub use client::{Client, DbConfig};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
