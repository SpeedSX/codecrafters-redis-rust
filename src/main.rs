#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Write, Read};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    const BUFFER_LENGTH: usize = 1024;

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                loop {
                    let mut buffer: Vec<u8> = vec![0; BUFFER_LENGTH];
                    let len = _stream.read(&mut buffer[..]).unwrap();
                
                    println!("Received: {}", String::from_utf8_lossy(&buffer[..len]));
                    if len > 0 {
                        _stream.write_all(b"+PONG\r\n").unwrap();
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
