use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};
use crate::{Start, Parsed};

pub struct OpReturn {
    sender : Sender<Option<Parsed>>,
    receiver : Receiver<Option<Parsed>>,
}

impl OpReturn {
    pub fn new() -> OpReturn {
        let (sender, receiver) = channel();
        OpReturn {
            sender,
            receiver,
        }
    }

}

impl Start for OpReturn {
    fn start(&self) {
        loop {
            let received = self.receiver.recv().unwrap();
            match received {
                Some(_received) => continue,
                None => break,
            }
        }
    }

    fn get_sender(&self) -> Sender<Option<Parsed>> {
        self.sender.clone()
    }
}