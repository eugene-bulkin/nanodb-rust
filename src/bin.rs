extern crate nanodb;

use nanodb::Client;

fn main() {
    let mut client = Client::new();

    client.run();
}
