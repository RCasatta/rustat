extern crate bitcoin;

use crate::op_return::OpReturn;
use crate::blocks::Blocks;
use crate::stats::Stats;
use crate::parse::TxOrBlock;
use crate::fee::Fee;
use std::io;
use std::thread;
use std::error::Error;

mod parse;
mod op_return;
mod blocks;
mod stats;
mod fee;

trait Startable {
    fn start(&self);
}

fn main() -> Result<(), Box<Error>> {
    let op_return = OpReturn::new();
    let stats = Stats::new();
    //let segwit = Segwit::new();
    let blocks = Blocks::new();
    let fee = Fee::new();

    let op_return_sender = op_return.get_sender();
    let stats_sender = stats.get_sender();
    let blocks_sender = blocks.get_sender();
    let fee_sender = fee.get_sender();
    //let segwit_sender = segwit.get_sender();

    let handle = thread::spawn( move ||  {
        let mut i = 0u64;
        loop {
            let mut buffer = String::new();
            match io::stdin().read_line(&mut buffer) {
                Ok(n) => {
                    if n == 0 {
                        println!("Received 0 as read_line after {} lines", i);
                        op_return_sender.send(TxOrBlock::End).expect("error sending End on op_return");
                        stats_sender.send(TxOrBlock::End).expect("error sending End on stats_sender");
                        //segwit_sender.send(TxOrBlock::End).expect("error sending End on segwit");
                        fee_sender.send(TxOrBlock::End).expect("error sending End on fee_sender");
                        blocks_sender.send(None).expect("error sending None to blocks_sender");

                        break;
                    }
                    match parse::line(&buffer) {
                        Ok(tx_or_block) => {
                            op_return_sender.send(tx_or_block.clone()).expect("failed to send tx_or_block to op_return");
                            stats_sender.send(tx_or_block.clone()).expect("failed to send tx_or_block to stats");
                            //segwit_sender.send(tx_or_block.clone()).expect("failed to send tx_or_block to segwit");
                            fee_sender.send(tx_or_block.clone()).expect("failed to send tx_or_block to fee");
                            match tx_or_block {
                                TxOrBlock::Block(block) => blocks_sender.send(Some(block)).expect("failed to send block to blocks"),
                                _ => (),
                            };
                        },
                        Err(e) => {
                            eprintln!("parse line error {:?} ({})", e, buffer);
                        },
                    };
                    i += 1;
                }
                Err(error) => {
                    eprintln!("Error: {}", error);
                }
            }
        }
        println!("ending stdin and line parser reader, {} lines read", i);
    });

    let mut startable : Vec<Box<Startable + Send>> = vec![Box::new(op_return),
                                                          Box::new(stats),
                                                          //Box::new(segwit),
                                                          Box::new(fee),
                                                          Box::new(blocks)];
    let mut processer = vec![];
    while let Some(el) = startable.pop() {
        let handle = thread::spawn(move|| {
            el.start();
        });
        processer.push(handle);
    }

    handle.join().expect("parse_handle failed to join");
    println!("parse_handle joined");

    while let Some(handle) = processer.pop() {
        println!("processer {:?} joining", handle);
        handle.join().expect("processer failed to join");
    }

    println!("end");
    Ok(())
}
