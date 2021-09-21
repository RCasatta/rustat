use bitcoin::blockdata::opcodes;
use bitcoin::consensus::serialize;
use bitcoin::{BlockHash, Script, Txid};
use chrono::DateTime;
use chrono::{Datelike, TimeZone, Utc};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Instant;
use time::Duration;
use blocks_iterator::BlockExtra;

pub struct Process {
    receiver: Receiver<Arc<Option<BlockExtra>>>,
    op_return_data: OpReturnData,
    script_type: ScriptType,
}

struct OpReturnData {
    op_ret_per_month: Vec<u64>,
    op_ret_size: BTreeMap<String, u64>, //pad with spaces usize of len up to 3
    op_ret_fee_per_month: Vec<u64>,
    op_ret_per_proto: HashMap<String, u64>,
    op_ret_per_proto_last_month: HashMap<String, u64>,
    op_ret_per_proto_last_year: HashMap<String, u64>,
    month_ago: u32,
    year_ago: u32,
}

struct ScriptType {
    all: Vec<u64>,
    p2pkh: Vec<u64>,
    p2pk: Vec<u64>,
    v0_p2wpkh: Vec<u64>,
    v0_p2wsh: Vec<u64>,
    p2sh: Vec<u64>,
    other: Vec<u64>,
    multisig: HashMap<String, u64>,
    multisig_tx: HashMap<String, String>,
}

impl Process {
    pub fn new(receiver: Receiver<Arc<Option<BlockExtra>>>) -> Process {
        Process {
            receiver,
            op_return_data: OpReturnData::new(),
            script_type: ScriptType::new(),
        }
    }

    pub fn start(&mut self) {
        let mut busy_time = 0u128;
        loop {
            let received = self.receiver.recv().expect("cannot receive fee block");
            match *received {
                Some(ref block) => {
                    let now = Instant::now();
                    self.process_block(&block);
                    busy_time = busy_time + now.elapsed().as_nanos();
                }
                None => break,
            }
        }

        //remove current month and cut initial months if not significant
        self.op_return_data.op_ret_per_month.pop();
        self.op_return_data.op_ret_per_month =
            self.op_return_data.op_ret_per_month[month_index("201501".to_string())..].to_vec();
        self.op_return_data.op_ret_fee_per_month.pop();
        self.op_return_data.op_ret_fee_per_month =
            self.op_return_data.op_ret_fee_per_month[month_index("201501".to_string())..].to_vec();
        self.op_return_data.op_ret_fee_per_month.pop();

        let toml = self.op_return_data.to_toml();
        //println!("{}", toml);
        fs::write("site/_data/op_return.toml", toml).expect("Unable to write file");

        self.script_type.all.pop();
        self.script_type.p2pkh.pop();
        self.script_type.p2pk.pop();
        self.script_type.p2sh.pop();
        self.script_type.v0_p2wpkh.pop();
        self.script_type.v0_p2wsh.pop();
        self.script_type.other.pop();
        let toml = self.script_type.to_toml();
        //println!("{}", toml);
        fs::write("site/_data/script_type.toml", toml).expect("Unable to write file");

        println!("{:?}", self.script_type.multisig_tx);

        println!(
            "ending processer, busy time: {}s",
            (busy_time / 1_000_000_000)
        );
    }

    fn process_block(&mut self, block: &BlockExtra) {
        let time = block.block.header.time;
        let date = Utc.timestamp(i64::from(time), 0);
        let index = date_index(date);

        for tx in block.block.txdata.iter() {
            for output in tx.output.iter() {
                if output.script_pubkey.is_op_return() {
                    self.process_op_return_script(
                        &output.script_pubkey,
                        time,
                        index,
                        block.tx_fee(&tx),
                    );
                }
                self.process_output_script(&output.script_pubkey, index);
            }
            for input in tx.input.iter() {
                if let Some(witness_script) = input.witness.last() {
                    if let Some(key) = parse_multisig(witness_script) {
                        if self.script_type.multisig_tx.get(&key).is_none() {
                            self.script_type
                                .multisig_tx
                                .insert(key.clone(), format!("{}", tx.txid()));
                        }
                        *self.script_type.multisig.entry(key).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    fn process_output_script(&mut self, script: &Script, index: usize) {
        self.script_type.all[index] += 1;
        if script.is_p2pkh() {
            self.script_type.p2pkh[index] += 1;
        } else if script.is_p2pk() {
            self.script_type.p2pk[index] += 1;
        } else if script.is_v0_p2wpkh() {
            self.script_type.v0_p2wpkh[index] += 1;
        } else if script.is_v0_p2wsh() {
            self.script_type.v0_p2wsh[index] += 1;
        } else if script.is_p2sh() {
            self.script_type.p2sh[index] += 1;
        } else {
            self.script_type.other[index] += 1;
        }
    }

    fn process_op_return_script(
        &mut self,
        op_return_script: &Script,
        time: u32,
        index: usize,
        fee: u64,
    ) {
        let script_bytes = op_return_script.as_bytes();
        let script_hex = hex::encode(script_bytes);
        let script_len = script_bytes.len();
        let data = &mut self.op_return_data;

        *data
            .op_ret_size
            .entry(format!("{:>3}", script_len))
            .or_insert(0) += 1;
        data.op_ret_per_month[index] += 1;
        data.op_ret_fee_per_month[index] += fee;

        if script_len > 4 {
            let op_ret_proto = if script_hex.starts_with("6a4c") && script_hex.len() > 5 {
                // 4c = OP_PUSHDATA1
                String::from(&script_hex[6..12])
            } else {
                String::from(&script_hex[4..10])
            };
            if time > data.year_ago {
                *data
                    .op_ret_per_proto_last_year
                    .entry(op_ret_proto.clone())
                    .or_insert(0) += 1;

                if time > data.month_ago {
                    *data
                        .op_ret_per_proto_last_month
                        .entry(op_ret_proto.clone())
                        .or_insert(0) += 1;
                }
            }
            *data
                .op_ret_per_proto
                .entry(op_ret_proto.clone())
                .or_insert(0) += 1;
        }
    }
}

impl ScriptType {
    fn new() -> Self {
        ScriptType {
            all: vec![0; month_array_len()],
            p2pkh: vec![0; month_array_len()],
            p2pk: vec![0; month_array_len()],
            v0_p2wpkh: vec![0; month_array_len()],
            v0_p2wsh: vec![0; month_array_len()],
            p2sh: vec![0; month_array_len()],
            other: vec![0; month_array_len()],
            multisig: HashMap::new(),
            multisig_tx: HashMap::new(),
        }
    }

    fn to_toml(&self) -> String {
        let mut s = String::new();

        s.push_str(&toml_section_vec("all", &self.all, None));
        s.push_str(&toml_section_vec("p2pkh", &self.p2pkh, None));
        s.push_str(&toml_section_vec("p2pk", &self.p2pk, None));
        s.push_str(&toml_section_vec("v0_p2wpkh", &self.v0_p2wpkh, None));
        s.push_str(&toml_section_vec("v0_p2wsh", &self.v0_p2wsh, None));
        s.push_str(&toml_section_vec("p2sh", &self.p2sh, None));
        s.push_str(&toml_section_vec("other", &self.other, None));
        s.push_str(&toml_section(
            "segwit_multisig",
            &hash_to_tree(&self.multisig),
        ));
        s.push_str(&toml_section(
            "segwit_multisig_other",
            &map_by_value(&self.multisig),
        ));

        s
    }
}

impl OpReturnData {
    fn new() -> OpReturnData {
        let month_ago = (Utc::now() - Duration::days(30)).timestamp() as u32; // 1 month ago
        let year_ago = (Utc::now() - Duration::days(365)).timestamp() as u32; // 1 year ago
        let len = month_array_len();
        OpReturnData {
            op_ret_per_month: vec![0; len],
            op_ret_size: BTreeMap::new(),
            op_ret_fee_per_month: vec![0; len],
            op_ret_per_proto: HashMap::new(),
            op_ret_per_proto_last_month: HashMap::new(),
            op_ret_per_proto_last_year: HashMap::new(),
            month_ago,
            year_ago,
        }
    }

    fn to_toml(&self) -> String {
        let mut s = String::new();

        s.push_str(&toml_section_vec(
            "op_ret_per_month",
            &self.op_ret_per_month,
            Some(month_index("201501".to_string())),
        ));
        s.push_str(&toml_section("op_ret_size", &self.op_ret_size));
        s.push_str(&toml_section(
            "op_ret_per_proto",
            &map_by_value(&self.op_ret_per_proto),
        ));
        s.push_str(&toml_section(
            "op_ret_per_proto_last_month",
            &map_by_value(&self.op_ret_per_proto_last_month),
        ));
        s.push_str(&toml_section(
            "op_ret_per_proto_last_year",
            &map_by_value(&self.op_ret_per_proto_last_year),
        ));

        s.push_str(&toml_section_vec_f64(
            "op_ret_fee_per_month",
            &convert_sat_to_bitcoin(&self.op_ret_fee_per_month),
            Some(month_index("201501".to_string())),
        ));

        s.push_str("\n[totals]\n");
        let op_ret_fee_total: u64 = self.op_ret_fee_per_month.iter().sum();
        s.push_str(&format!(
            "op_ret_fee = {}\n",
            (op_ret_fee_total as f64 / 100_000_000f64)
        ));

        s
    }
}

pub fn filter_key(block_hash: BlockHash) -> Vec<u8> {
    let mut v = vec![];
    v.push(b'f');
    v.extend(serialize(&block_hash));
    v
}

pub fn parse_multisig(witness_script: &Vec<u8>) -> Option<String> {
    let witness_script_len = witness_script.len();
    if witness_script.last() == Some(&opcodes::all::OP_CHECKMULTISIG.into_u8())
        && witness_script_len > 1
    {
        let n = read_pushnum(witness_script[0]);
        let m = read_pushnum(witness_script[witness_script_len - 2]);
        if n.is_some() && m.is_some() {
            return Some(format!("{:02}of{:02}", n.unwrap(), m.unwrap()));
        }
    }
    None
}

pub fn read_pushnum(value: u8) -> Option<u8> {
    if value >= opcodes::all::OP_PUSHNUM_1.into_u8()
        && value <= opcodes::all::OP_PUSHNUM_16.into_u8()
    {
        Some(value - opcodes::all::OP_PUSHNUM_1.into_u8() + 1)
    } else {
        None
    }
}

pub fn convert_sat_to_bitcoin(map: &Vec<u64>) -> Vec<f64> {
    map.iter().map(|v| *v as f64 / 100_000_000f64).collect()
}

pub fn toml_section_vec_f64(title: &str, vec: &Vec<f64>, shift: Option<usize>) -> String {
    let mut s = String::new();
    s.push_str(&format!("\n[{}]\n", title));
    let labels: Vec<String> = vec
        .iter()
        .enumerate()
        .map(|el| index_month(el.0 + shift.unwrap_or(0)))
        .collect();
    s.push_str(&format!("labels={:?}\n", labels));
    s.push_str(&format!("values={:?}\n\n", vec));
    s
}

pub fn toml_section_vec(title: &str, vec: &Vec<u64>, shift: Option<usize>) -> String {
    let mut s = String::new();
    s.push_str(&format!("\n[{}]\n", title));
    let labels: Vec<String> = vec
        .iter()
        .enumerate()
        .map(|el| index_month(el.0 + shift.unwrap_or(0)))
        .collect();
    s.push_str(&format!("labels={:?}\n", labels));
    s.push_str(&format!("values={:?}\n\n", vec));
    s
}

pub fn toml_section(title: &str, map: &BTreeMap<String, u64>) -> String {
    let mut s = String::new();
    s.push_str(&format!("\n[{}]\n", title));
    let labels: Vec<String> = map.keys().cloned().collect();
    s.push_str(&format!("labels={:?}\n", labels));
    let values: Vec<u64> = map.values().cloned().collect();
    s.push_str(&format!("values={:?}\n\n", values));
    s
}

pub fn hash_to_tree(map: &HashMap<String, u64>) -> BTreeMap<String, u64> {
    let mut tree: BTreeMap<String, u64> = BTreeMap::new();
    for (key, value) in map.iter() {
        tree.insert(key.to_string(), *value);
    }
    tree
}

pub fn map_by_value(map: &HashMap<String, u64>) -> BTreeMap<String, u64> {
    let mut tree: BTreeMap<String, u64> = BTreeMap::new();
    let mut count_vec: Vec<(&String, &u64)> = map.iter().collect();
    count_vec.sort_by(|a, b| b.1.cmp(a.1));
    for (key, value) in count_vec.iter().take(10) {
        tree.insert(key.to_string(), **value);
    }
    let other = count_vec.iter().skip(10).fold(0, |acc, x| acc + x.1);
    if other > 0 {
        tree.insert("other".to_owned(), other);
    }
    tree
}

pub fn cumulative(values: &Vec<u64>) -> Vec<u64> {
    let mut result = Vec::with_capacity(values.len());
    let mut cum = 0;
    for val in values {
        cum += val;
        result.push(cum);
    }
    result
}

pub fn toml_section_hash(title: &str, value: &(u64, Option<Txid>)) -> String {
    let mut s = String::new();
    s.push_str(&format!("\n[{}]\n", title));
    s.push_str(&format!("hash=\"{:?}\"\n", value.1.unwrap()));
    s.push_str(&format!("value={:?}\n\n", value.0));

    s
}

pub fn toml_section_block_hash(title: &str, value: &(u64, Option<BlockHash>)) -> String {
    let mut s = String::new();
    s.push_str(&format!("\n[{}]\n", title));
    s.push_str(&format!("hash=\"{:?}\"\n", value.1.unwrap()));
    s.push_str(&format!("value={:?}\n\n", value.0));

    s
}

pub fn encoded_length_7bit_varint(mut value: u64) -> u64 {
    let mut bytes = 1;
    loop {
        if value <= 0x7F {
            return bytes;
        }
        bytes += 1;
        value >>= 7;
    }
}

pub fn compress_amount(n: u64) -> u64 {
    let mut n = n;
    if n == 0 {
        return 0;
    }
    let mut e = 0u64;
    loop {
        if (n % 10) != 0 || e >= 9 {
            break;
        }
        n /= 10;
        e += 1;
    }
    if e < 9 {
        let d = n % 10;
        assert!(d >= 1 && d <= 9);
        n /= 10;
        1 + (n * 9 + d - 1) * 10 + e
    } else {
        1 + ((n - 1) * 10) + 9
    }
}

#[cfg(test)]
pub fn decompress_amount(x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    let mut x = x;
    x -= 1;
    let mut e = x % 10;
    x /= 10;
    let mut n;
    if e < 9 {
        let d = (x % 9) + 1;
        x /= 9;
        n = x * 10 + d;
    } else {
        n = x + 1;
    }
    loop {
        if e == 0 {
            break;
        }
        n *= 10;
        e -= 1;
    }
    n
}

pub fn date_index(date: DateTime<Utc>) -> usize {
    return (date.year() as usize - 2009) * 12 + (date.month() as usize - 1);
}

pub fn index_month(index: usize) -> String {
    let year = 2009 + index / 12;
    let month = (index % 12) + 1;
    format!("{:04}{:02}", year, month)
}

pub fn month_date(yyyymm: String) -> DateTime<Utc> {
    let year: i32 = yyyymm[0..4].parse().unwrap();
    let month: u32 = yyyymm[4..6].parse().unwrap();
    Utc.ymd(year, month, 1).and_hms(0, 0, 0)
}

pub fn month_index(yyyymm: String) -> usize {
    date_index(month_date(yyyymm))
}

pub fn month_array_len() -> usize {
    date_index(Utc::now()) + 1
}

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
