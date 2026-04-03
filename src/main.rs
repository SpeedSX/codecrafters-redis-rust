#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Write, Read};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let mut buffer: Vec<u8> = Vec::new();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                _stream.read_to_end(&mut buffer).unwrap();
                
                let message = String::from_utf8_lossy(&buffer);

                for _command in message.split_whitespace() {
                    _stream.write_all(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
