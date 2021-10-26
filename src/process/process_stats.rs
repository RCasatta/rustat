use crate::process::{compress_amount, date_index, encoded_length_7bit_varint, month_array_len};
use blocks_iterator::bitcoin::blockdata::script::Instruction;
use blocks_iterator::bitcoin::consensus::{deserialize, encode, Decodable};
use blocks_iterator::bitcoin::hashes::hex::FromHex;
use blocks_iterator::bitcoin::{BlockHash, SigHashType, Transaction, Txid, VarInt};
use blocks_iterator::BlockExtra;
use chrono::{TimeZone, Utc};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Instant;

pub struct ProcessStats {
    receiver: Receiver<Arc<Option<BlockExtra>>>,
    pub stats: Stats,
}

pub struct Stats {
    pub max_outputs_per_tx: (u64, Option<Txid>),
    pub min_weight_tx: (u64, Option<Txid>),
    pub max_inputs_per_tx: (u64, Option<Txid>),
    pub max_weight_tx: (u64, Option<Txid>),
    pub total_outputs: u64,
    pub total_inputs: u64,
    pub amount_over_32: usize,
    pub rounded_amount: u64,
    pub max_block_size: (u64, Option<BlockHash>),
    pub max_tx_per_block: (u64, Option<BlockHash>),
    pub min_hash: BlockHash,
    pub total_spent_in_block: u64,
    pub total_spent_in_block_per_month: Vec<u64>,
    pub total_bytes_output_value_varint: u64,
    pub total_bytes_output_value_compressed_varint: u64,
    pub total_bytes_output_value_bitcoin_varint: u64,
    pub total_bytes_output_value_compressed_bitcoin_varint: u64,
    pub rounded_amount_per_month: Vec<u64>,
    pub block_size_per_month: Vec<u64>,
    pub sighashtype: HashMap<String, u64>,
    pub in_out: HashMap<String, u64>,
    pub sighash_file: File,
    pub fee_file: File,
    pub blocks_len_file: File,
    pub total_outputs_per_month: Vec<u64>,
    pub total_inputs_per_month: Vec<u64>,
    pub total_tx_per_month: Vec<u64>,
    pub fee_per_month: Vec<u64>,

    /// number of inputs using witness (number of element > 0) and not using witness
    pub has_witness: HashMap<String, u64>,
    /// number of witness elements
    pub witness_elements: HashMap<String, u64>,
    /// witness byte size as sum of len of every element
    pub witness_byte_size: HashMap<String, u64>,
}

//TODO split again this one slower together with read
impl ProcessStats {
    pub fn new(receiver: Receiver<Arc<Option<BlockExtra>>>) -> ProcessStats {
        ProcessStats {
            receiver,
            stats: Stats::new(),
        }
    }

    pub fn start(mut self) -> Stats {
        let mut busy_time = 0u128;
        let mut now = Instant::now();
        loop {
            busy_time += now.elapsed().as_nanos();
            let received = self.receiver.recv().expect("cannot receive fee block");
            now = Instant::now();
            match *received {
                Some(ref block) => {
                    self.process_block(&block);
                }
                None => break,
            }
        }

        self.stats.total_spent_in_block_per_month.pop();
        self.stats.rounded_amount_per_month.pop();
        self.stats.block_size_per_month.pop();
        self.stats.total_inputs_per_month.pop();
        self.stats.total_outputs_per_month.pop();
        self.stats.total_tx_per_month.pop();
        self.stats.fee_per_month.pop();
        let not_using = self.stats.witness_elements.remove("00").unwrap();
        let using = self.stats.witness_elements.values().sum();
        self.stats.has_witness.insert("with".to_string(), using);
        self.stats
            .has_witness
            .insert("without".to_string(), not_using);

        self.stats.witness_byte_size.remove("000");

        busy_time += now.elapsed().as_nanos();
        println!(
            "ending stats processer, busy time: {}s",
            (busy_time / 1_000_000_000)
        );

        self.stats
    }

    fn process_block(&mut self, block: &BlockExtra) {
        let time = block.block.header.time;
        let date = Utc.timestamp(i64::from(time), 0);
        let index = date_index(date);

        self.stats.block_size_per_month[index] += block.size as u64;
        let mut fees_from_this_block = vec![];
        let tx_hashes: HashSet<_> = block.block.txdata.iter().map(|tx| tx.txid()).collect();
        for tx in block.block.txdata.iter() {
            for output in tx.output.iter() {
                let len = VarInt(output.value).len() as u64;

                self.stats.total_bytes_output_value_bitcoin_varint += len;
                self.stats.total_bytes_output_value_varint +=
                    encoded_length_7bit_varint(output.value);
                let compressed = compress_amount(output.value);
                self.stats
                    .total_bytes_output_value_compressed_bitcoin_varint +=
                    VarInt(compressed).len() as u64;
                self.stats.total_bytes_output_value_compressed_varint +=
                    encoded_length_7bit_varint(compressed);
                if (output.value % 1000) == 0 {
                    self.stats.rounded_amount_per_month[index] += 1;
                    self.stats.rounded_amount += 1;
                }
            }
            let mut strange_sighash = vec![];
            let mut count_inputs_in_block = 0;

            for input in tx.input.iter() {
                if tx_hashes.contains(&input.previous_output.txid) {
                    self.stats.total_spent_in_block += 1;
                    self.stats.total_spent_in_block_per_month[index] += 1;
                    count_inputs_in_block += 1;
                }

                for instr in input.script_sig.instructions() {
                    if let Ok(Instruction::PushBytes(data)) = instr {
                        if let Ok(sighash) = deserialize::<SignatureHash>(data) {
                            *self
                                .stats
                                .sighashtype
                                .entry(format!("{:?}", sighash.0))
                                .or_insert(0) += 1;
                            match sighash.0 {
                                SigHashType::All | SigHashType::AllPlusAnyoneCanPay => (),
                                _ => strange_sighash.push((sighash.0, input.sequence)),
                            };
                        }
                    }
                }
                *self
                    .stats
                    .witness_elements
                    .entry(format!("{:02}", input.witness.len()))
                    .or_insert(0) += 1;
                *self
                    .stats
                    .witness_byte_size
                    .entry(format!(
                        "{:03}",
                        input.witness.iter().map(|e| e.len()).sum::<usize>()
                    ))
                    .or_insert(0) += 1;

                for vec in input.witness.iter() {
                    if let Ok(sighash) = deserialize::<SignatureHash>(vec) {
                        *self
                            .stats
                            .sighashtype
                            .entry(format!("{:?}", sighash.0))
                            .or_insert(0) += 1;
                        match sighash.0 {
                            SigHashType::All | SigHashType::AllPlusAnyoneCanPay => (),
                            _ => strange_sighash.push((sighash.0, input.sequence)),
                        };
                    }
                }
            }
            if !strange_sighash.is_empty() {
                self.stats
                    .sighash_file
                    .write(format!("{} {:?}\n", tx.txid(), strange_sighash).as_bytes())
                    .unwrap();
            }
            if count_inputs_in_block == tx.input.len() {
                fees_from_this_block.push(block.tx_fee(&tx).unwrap())
            }
            self.process_stats(&tx, index);
        }
        let tx_len = block.block.txdata.len();
        let tx_with_fee_in_block_len = fees_from_this_block.len();
        let fee = block.fee().unwrap();
        let average_fee = fee as f64 / tx_len as f64;
        let estimated_average_fee = if tx_with_fee_in_block_len == 0 {
            0f64
        } else {
            fees_from_this_block.iter().sum::<u64>() as f64 / tx_with_fee_in_block_len as f64
        };
        let estimated_fee = (estimated_average_fee * tx_len as f64) as u64;
        self.stats.fee_per_month[index] += fee;
        self.stats
            .fee_file
            .write(
                format!(
                    "{},{},{},{},{},{},{}\n",
                    block.height,
                    tx_len,
                    fee,
                    average_fee,
                    tx_with_fee_in_block_len,
                    estimated_fee,
                    estimated_average_fee
                )
                .as_bytes(),
            )
            .unwrap();

        let hash = block.block.header.block_hash();
        if self.stats.min_hash > hash {
            self.stats.min_hash = hash;
        }
        let size = u64::from(block.size);
        if self.stats.max_block_size.0 < size {
            self.stats.max_block_size = (size, Some(hash));
        }

        let l = block.block.txdata.len() as u64;
        self.stats
            .blocks_len_file
            .write(format!("{}\n", l).as_bytes())
            .unwrap();
        if self.stats.max_tx_per_block.0 < l {
            self.stats.max_tx_per_block = (l, Some(hash));
        }
    }

    fn process_stats(&mut self, tx: &Transaction, index: usize) {
        let weight = tx.get_weight() as u64;
        let outputs = tx.output.len() as u64;
        let inputs = tx.input.len() as u64;
        self.stats.total_outputs_per_month[index] += outputs;
        self.stats.total_inputs_per_month[index] += inputs;
        self.stats.total_tx_per_month[index] += 1;
        let txid = tx.txid();
        self.stats.total_outputs += outputs as u64;
        self.stats.total_inputs += inputs as u64;
        if self.stats.max_outputs_per_tx.0 < outputs {
            self.stats.max_outputs_per_tx = (outputs, Some(txid));
        }
        if self.stats.max_inputs_per_tx.0 < inputs {
            self.stats.max_inputs_per_tx = (inputs, Some(txid));
        }
        if self.stats.max_weight_tx.0 < weight {
            self.stats.max_weight_tx = (weight, Some(txid));
        }
        if self.stats.min_weight_tx.0 > weight {
            self.stats.min_weight_tx = (weight, Some(txid));
        }

        let in_out_key = format!("{:02}-{:02}", inputs, outputs);
        *self.stats.in_out.entry(in_out_key).or_insert(0) += 1;

        self.stats.amount_over_32 += tx.output.iter().filter(|o| o.value > 0xffff_ffff).count();
    }
}

impl Stats {
    pub fn new() -> Self {
        let sighash_file = File::create("sighashes.txt").unwrap();
        let fee_file = File::create("fee.txt").unwrap();
        let blocks_len_file = File::create("blocks_len.txt").unwrap();
        Stats {
            max_outputs_per_tx: (100u64, None),
            max_inputs_per_tx: (100u64, None),
            min_weight_tx: (10000u64, None),
            max_weight_tx: (0u64, None),
            total_outputs: 0u64,
            total_inputs: 0u64,
            amount_over_32: 0usize,
            rounded_amount: 0u64,
            total_spent_in_block: 0u64,
            max_block_size: (0u64, None),
            max_tx_per_block: (0u64, None),
            min_hash: BlockHash::from_hex(
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
            )
            .unwrap(),
            total_bytes_output_value_varint: 0u64,
            total_bytes_output_value_compressed_varint: 0u64,
            total_bytes_output_value_bitcoin_varint: 0u64,
            total_bytes_output_value_compressed_bitcoin_varint: 0u64,
            total_spent_in_block_per_month: vec![0u64; month_array_len()],
            rounded_amount_per_month: vec![0u64; month_array_len()],

            block_size_per_month: vec![0u64; month_array_len()],
            sighashtype: HashMap::new(),
            witness_elements: HashMap::new(),
            witness_byte_size: HashMap::new(),
            has_witness: HashMap::new(),
            in_out: HashMap::new(),
            sighash_file,
            fee_file,
            blocks_len_file,
            total_inputs_per_month: vec![0u64; month_array_len()],
            total_outputs_per_month: vec![0u64; month_array_len()],
            total_tx_per_month: vec![0u64; month_array_len()],
            fee_per_month: vec![0u64; month_array_len()],
        }
    }
}

struct SignatureHash(pub SigHashType);

impl Decodable for SignatureHash {
    fn consensus_decode<D: std::io::Read>(mut d: D) -> Result<Self, encode::Error> {
        let first = u8::consensus_decode(&mut d)?;
        if first != 0x30 {
            return Err(encode::Error::ParseFailed("Signature must start with 0x30"));
        }
        let _ = u8::consensus_decode(&mut d)?;
        let integer_header = u8::consensus_decode(&mut d)?;
        if integer_header != 0x02 {
            return Err(encode::Error::ParseFailed("No integer header"));
        }
        let length_r = u8::consensus_decode(&mut d)?;
        for _ in 0..length_r {
            let _ = u8::consensus_decode(&mut d)?;
        }
        let integer_header = u8::consensus_decode(&mut d)?;
        if integer_header != 0x02 {
            return Err(encode::Error::ParseFailed("No integer header"));
        }
        let length_s = u8::consensus_decode(&mut d)?;
        for _ in 0..length_s {
            let _ = u8::consensus_decode(&mut d)?;
        }

        let sighash_u8 = u8::consensus_decode(&mut d)?;
        let sighash = SigHashType::from_u32_consensus(sighash_u8 as u32);

        Ok(SignatureHash(sighash))
    }
}

/*
#[cfg(test)]
mod test {
    use crate::process::cumulative;
    use crate::process::decompress_amount;
    use crate::process::index_month;
    use crate::process::parse_multisig;
    use crate::process::{compress_amount, SignatureHash};
    use crate::process::{date_index, encoded_length_7bit_varint, month_date};
    use bitcoin::consensus::{deserialize, Decodable};
    use bitcoin::SigHashType;
    use chrono::offset::TimeZone;
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn test0() {
        let date = Utc.timestamp(1230768000i64, 0);
        assert_eq!(0, date_index(date));
        assert_eq!("200901", index_month(0));
        let date = Utc.timestamp(1262304000i64, 0);
        assert_eq!(12, date_index(date));
        assert_eq!("200912", index_month(11));
        assert_eq!("201001", index_month(12));
        for i in 0..2000 {
            assert_eq!(i, date_index(month_date(index_month(i))));
        }
    }

    #[test]
    fn test1() {
        assert_eq!(encoded_length_7bit_varint(127), 1);
        assert_eq!(encoded_length_7bit_varint(128), 2);
        assert_eq!(encoded_length_7bit_varint(1_270), 2);
        assert_eq!(encoded_length_7bit_varint(111_270), 3);
        assert_eq!(encoded_length_7bit_varint(2_097_151), 3);
        assert_eq!(encoded_length_7bit_varint(2_097_152), 4);
    }

    #[test]
    fn test2() {
        let tuples = vec![("one", 1), ("two", 2), ("three", 3)];
        let m: HashMap<_, _> = tuples.into_iter().collect();
        println!("{:?}", m);
    }

    #[test]
    fn test3() {
        let mut b: bool = true;
        let u = b as u32;
        assert_eq!(u, 1u32);
        b = false;
        let u = b as u32;
        assert_eq!(u, 0u32);
    }

    #[test]
    fn test4() {
        let i = 10_000_000_000;
        let compressed = compress_amount(i);
        println!("compressed: {}", compressed);
        assert_eq!(i, decompress_amount(compressed));

        for i in 0..std::u64::MAX {
            assert_eq!(i, decompress_amount(compress_amount(i)));
        }
    }

    #[test]
    fn test5() {
        let vec = vec![1, 1, 1];
        assert_eq!(cumulative(&vec), vec![1, 2, 3]);
    }

    #[test]
    fn test_parse_multisig() {
        let script = hex::decode("52210293de2378b245e0c4a8325d2beb2e537041a3b9b12c96052a9f30954700e56ef3210230d013baf38205252c298625a7c7799e1f11a016d3738198410bcf8bcc1fecab52ae").unwrap();
        assert_eq!(Some("02of02".to_string()), parse_multisig(&script));
    }

    #[test]
    fn test_decode_signature() {
        let der_signature = hex::decode("3045022100bd3688bbeefe67dbaf34b7e7d250bcbcf99c8a5cf7cb680393f5025b03dac912022057dbf2317c3413b57eeaf712f1599b74213f1a4ea4e3f5091db6f7fe8d02465a01").unwrap();

        let signatureHash: SignatureHash = deserialize(&der_signature).unwrap();
        assert_eq!(signatureHash.0, SigHashType::All);

        let der_signature = hex::decode("3045022100bd3688bbeefe67dbaf34b7e7d250bcbcf99c8a5cf7cb680393f5025b03dac912022057dbf2317c3413b57eeaf712f1599b74213f1a4ea4e3f5091db6f7fe8d02465a83").unwrap();

        let signatureHash: SignatureHash = deserialize(&der_signature).unwrap();
        assert_eq!(signatureHash.0, SigHashType::SinglePlusAnyoneCanPay);
    }
}

 */