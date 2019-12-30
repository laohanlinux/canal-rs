#[macro_use] extern crate log;
#[macro_use] extern crate failure;
#[macro_use] extern crate serde_derive;

pub mod client;
pub mod cluster;
pub mod protobuf;

pub use client::{Client, Config};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
