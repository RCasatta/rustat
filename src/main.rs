#![recursion_limit="128"]

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate serde_json;

extern crate bitcoin;
extern crate chrono;
extern crate time;
extern crate handlebars;
extern crate rustc_serialize;
extern crate num_cpus;

use handlebars::Handlebars;
use std::io::{self, Read};
use chrono::{Utc, TimeZone, Datelike, DateTime};
use std::collections::HashMap;
use time::Duration;
use std::fs::File;
use bitcoin::blockdata::script::Script;
use std::fmt::Write as FmtWrite;
use std::io::Write as IoWrite;
use rustc_serialize::hex::FromHex;
use std::sync::mpsc::channel;
use std::sync::mpsc::sync_channel;
use std::thread;
use std::fs;

error_chain! {}

struct Parsed {
    ym : String,
    script : String,
    is_last_month : bool,
    is_last_year : bool,
    is_segwit : bool,
    op_ret_proto: Option<String>,
    script_size: usize,
}

struct Maps {
    op_ret_per_month : HashMap<String,u32>,
    op_ret_per_proto : HashMap<String,u32>,
    op_ret_per_proto_last_month : HashMap<String,u32>,
    op_ret_per_proto_last_year : HashMap<String,u32>,
    op_ret_size : HashMap<usize,u32>,
    txo_per_month : HashMap<String,u32>,
    segwit_per_month : HashMap<String,u32>,
    tx_per_template : HashMap<String,u32>,
    tx_per_template_last_month : HashMap<String,u32>,
    txo_size : HashMap<usize,u32>,
    veriblock_per_month : HashMap<String,u32>,
}

#[derive(Debug)]
struct Serie {
    labels: String,
    data: String,
}

impl Maps {
    fn new() -> Maps {
        Maps {
            op_ret_per_month : HashMap::new(),
            op_ret_per_proto : HashMap::new(),
            op_ret_per_proto_last_month : HashMap::new(),
            op_ret_per_proto_last_year : HashMap::new(),
            op_ret_size : HashMap::new(),
            txo_per_month : HashMap::new(),
            segwit_per_month : HashMap::new(),
            tx_per_template : HashMap::new(),
            tx_per_template_last_month : HashMap::new(),
            txo_size : HashMap::new(),
            veriblock_per_month : HashMap::new(),
        }
    }
}

fn run() -> Result<()> {
    let op_return_template = read_template("templates/opreturn.html");
    let segwit_template = read_template("templates/segwit.html");
    let tx_template = read_template("templates/template.html");

    let mut maps = Maps::new();
    let month_ago = Utc::now() - Duration::days(30); // 1 month ago
    let year_ago = Utc::now() - Duration::days(365); // 1 year ago


    let num = num_cpus::get_physical();
    eprintln!("num_cpus {}",num);

    let mut senders = vec!();
    let mut handles = vec!();
    let (parsed_sender, parsed_receiver) = channel();
    for _i in 0..num {
        let (sender, receiver) = sync_channel(1000);
        let cloned_parsed_sender = parsed_sender.clone();

        let handle = thread::spawn(move|| {
            loop {
                let received = receiver.recv().unwrap();
                match received {
                    Some(value) => {
                        if let Some(parsed) = parse_row(value, &month_ago, &year_ago) {
                            let _r = cloned_parsed_sender.send(Some(parsed));
                        };

                    },
                    None => {
                        let _r = cloned_parsed_sender.send(None);
                        break
                    },
                }
            }
        });
        handles.push(handle);
        senders.push(sender);
    }

    let updater_handle = thread::Builder::new()
        .name("r".into())
        .spawn(move|| {
            let mut none_count=0;
            loop {
                match parsed_receiver.recv().unwrap() {
                    Some(result) => update(result, &mut maps),
                    None => none_count += 1,
                };
                if none_count>=num {
                    break
                }
            }

            //remove current month
            let now = Utc::now();
            let current_ym = format!("{}{:02}", now.year(), now.month());
            maps.op_ret_per_month.remove(&current_ym);
            maps.segwit_per_month.remove(&current_ym);
            maps.txo_per_month.remove(&current_ym);
            maps.veriblock_per_month.remove(&current_ym);

            //align key space
            align(&mut  maps.op_ret_per_month, &mut maps.txo_per_month);
            align(&mut  maps.segwit_per_month, &mut maps.txo_per_month);
            align(&mut  maps.veriblock_per_month, &mut maps.txo_per_month);

            let txo_per_month : Serie = print_map_by_key(&maps.txo_per_month);
            let op_ret_per_month : Serie = print_map_by_key(&maps.op_ret_per_month);
            let segwit_per_month : Serie = print_map_by_key(&maps.segwit_per_month);

            let op_ret_per_proto : Serie  = print_map_by_value(&maps.op_ret_per_proto);
            let op_ret_per_proto_last_month : Serie  = print_map_by_value(&maps.op_ret_per_proto_last_month);
            let op_ret_per_proto_last_year : Serie  = print_map_by_value(&maps.op_ret_per_proto_last_year);
            let tx_per_template = print_map_by_value(&maps.tx_per_template);
            let tx_per_template_last_month = print_map_by_value(&maps.tx_per_template_last_month);

            let txo_size : Serie = print_map_by_usize_key(&maps.txo_size);
            let op_ret_size : Serie = print_map_by_usize_key(&maps.op_ret_size);

            let veriblock_per_month : Serie = print_map_by_key(&maps.veriblock_per_month);

            let json = json!({
                     "op_ret_per_month_labels":op_ret_per_month.labels,
                     "op_ret_per_month_data":op_ret_per_month.data,
                     "op_ret_per_proto_labels":op_ret_per_proto.labels,
                     "op_ret_per_proto_data":op_ret_per_proto.data,
                     "op_ret_per_proto_last_month_labels":op_ret_per_proto_last_month.labels,
                     "op_ret_per_proto_last_month_data":op_ret_per_proto_last_month.data,
                     "op_ret_per_proto_last_year_labels":op_ret_per_proto_last_year.labels,
                     "op_ret_per_proto_last_year_data":op_ret_per_proto_last_year.data,
                     "tx_per_template_labels":tx_per_template.labels,
                     "tx_per_template_data":tx_per_template.data,
                     "tx_per_template_last_month_labels":tx_per_template_last_month.labels,
                     "tx_per_template_last_month_data":tx_per_template_last_month.data,
                     "txo_per_month_labels":txo_per_month.labels,
                     "txo_per_month_data":txo_per_month.data,
                     "segwit_per_month_labels":segwit_per_month.labels,
                     "segwit_per_month_data":segwit_per_month.data,
                     "txo_size_labels":txo_size.labels,
                     "txo_size_data":txo_size.data,
                     "op_ret_size_labels":op_ret_size.labels,
                     "op_ret_size_data":op_ret_size.data,
                     "veriblock_per_month_labels":veriblock_per_month.labels,
                     "veriblock_per_month_data":veriblock_per_month.data,
                     });

            write_output(&op_return_template, &json, "outputs/op_return/");
            write_output(&segwit_template, &json, "outputs/segwit/");
            write_output(&tx_template, &json, "outputs/template/");

        }).unwrap();

    let mut i = 0;
    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(n) => {
                if n == 0 {
                    break;
                }

                senders[i].send(Some(buffer)).unwrap();
                i=(i+1)%num;

            }
            Err(error) => panic!("error: {}", error),
        }
    }
    for sender in senders {
        let _r = sender.send(None);
    }
    for handle in handles {
        let _j = handle.join();
    }

    let _j = updater_handle.join();

    Ok(())
}

fn write_output(template : &str, json : &serde_json::Value, dir : &str) {
    let reg = Handlebars::new();
    let mut buffer : String = String::new();
    write!(&mut buffer, "{}",
           reg.template_render(&template, &json).unwrap()
    ).unwrap();
    fs::create_dir_all(dir).unwrap();
    let path = format!("{}/index.html", dir);
    let mut result_html: File = File::create(&path).expect(&format!("error opening output {}",path));
    let _r = result_html.write_all(buffer.as_bytes());
}


fn read_template(name : &str) -> String {
    let mut template = File::open(name).expect("template not found");
    let mut template_content = String::new();
    template.read_to_string(&mut template_content).expect(&format!("something went wrong reading the template file: '{}'", name));
    template_content
}



fn print_map_by_value(map : &HashMap<String,u32>) -> Serie {
    let mut count_vec: Vec<(&String, &u32)> = map.iter().collect();
    count_vec.sort_by(|a, b| b.1.cmp(a.1));
    let mut name : Vec<String> = vec!();
    let mut value : Vec<u32> = vec!();
    let mut other = 0;
    let mut i = 0;
    for (a,b) in count_vec {
        if i<10 {
            name.push(a.to_owned());
            value.push(b.clone());
        } else {
            other += b;
        }
        i += 1;
    }
    name.push("other".to_owned());
    value.push(other);
    let ret = Serie {
        labels: str::replace(&format!("{:?}",name),"\"","'"),
        data: format!("{:?}", value),
    };
    println!("{:?}", ret);
    ret
}

fn align (map1 : &mut HashMap<String,u32>, map2 : &mut HashMap<String,u32>) {
    for key in map1.keys() {
        if let None = map2.get(key) {
            map2.insert(key.to_owned(),0);
        }
    }

    for key in map2.keys() {
        if let None = map1.get(key) {
            map1.insert(key.to_owned(),0);
        }
    }
}


fn print_map_by_usize_key(map : &HashMap<usize,u32>) -> Serie {
    let mut map_keys : Vec<_> = map.keys().collect();
    map_keys.sort();
    let mut keys : Vec<usize> = vec!();
    let mut values : Vec<u32> = vec!();
    for key in map_keys {
        let value = map.get(key).unwrap();
        keys.push(key.to_owned());
        values.push(value.clone());

    }
    let ret = Serie {
        labels: str::replace(&format!("{:?}",keys),"\"","'"),
        data: format!("{:?}",values),
    };
    println!("{:?}", ret);
    ret
}

fn print_map_by_key(map : &HashMap<String,u32>) -> Serie {
    let mut map_keys : Vec<_> = map.keys().collect();
    map_keys.sort();
    let mut keys : Vec<String> = vec!();
    let mut values : Vec<u32> = vec!();
    for key in map_keys {
        let value = map.get(key).unwrap();
        println!("value {} {}", key, value);
        keys.push(key.to_owned());
        values.push(value.clone());

    }
    println!("");

    Serie {
        labels: str::replace(&format!("{:?}",keys),"\"","'"),
        data: format!("{:?}",values),
    }
}

fn parse_script(script : &bitcoin::blockdata::script::Script) -> String {
    let script = &format!("{}", script);
    let script = str::replace(&script,"Script(","");
    let script = str::replace(&script,")","");
    let mut buffer = String::new();
    for el in script.split_whitespace() {
        if el.starts_with("OP_") {
            if el.starts_with("OP_PUSHBYTES") {
                buffer.push_str("OP_PUSHBYTES");
            } else {
                buffer.push_str(el);
            }
        } else {
            buffer.push_str("(DATA)");
        }
        buffer.push_str(" ");
    }
    buffer
}



fn parse_row(el : String, month_ago : &DateTime<Utc>, year_ago : &DateTime<Utc>) -> Option<Parsed> {
    let mut x = el.split_whitespace();
    let timestamp = x.next();
    let value = x.next();
    if let (Some(timestamp),Some(value)) = (timestamp,value) {
        //println!("{} {}", timestamp, value);
        let timestamp = timestamp.parse::<i64>().expect("found non parsable timestamp");
        let date = Utc.timestamp(timestamp, 0);

        let ym = format!("{}{:02}", date.year(), date.month());
        let script_bytes = value.from_hex().unwrap();
        let script_size = script_bytes.len();
        let script = Script::from(script_bytes);
        let script = parse_script(&script);
        let is_last_month = date > *month_ago;
        let is_last_year = date > *year_ago;
        let is_segwit = value.starts_with("0014") ||  value.starts_with("0020");

        let op_ret_proto = if value.starts_with("6a") && value.len() > 9 { // 6a = OP_RETURN
            if value.starts_with("6a4c") && value.len() > 11 {  // 4c = OP_PUSHDATA1
                Some(String::from(&value[6..12]))
            } else {
                Some(String::from(&value[4..10]))
            }
        } else {
            None
        };


        Some(
            Parsed {
                ym,
                script,
                op_ret_proto,
                is_last_month,
                is_last_year,
                is_segwit,
                script_size,
            }
        )
    } else {
        None
    }

}

fn update(parsed : Parsed, maps :  &mut Maps) {
    if parsed.is_segwit {
        *maps.segwit_per_month.entry(parsed.ym.clone()).or_insert(0)+=1;
    }

    if let Some(op_ret_proto) = parsed.op_ret_proto {
        if parsed.is_last_month {
            *maps.op_ret_per_proto_last_month.entry(op_ret_proto.clone()).or_insert(0) += 1;
        }
        if parsed.is_last_year {
            *maps.op_ret_per_proto_last_year.entry(op_ret_proto.clone()).or_insert(0) += 1;
        }
        *maps.op_ret_per_month.entry(parsed.ym.clone()).or_insert(0)+=1;
        *maps.op_ret_per_proto.entry(op_ret_proto.clone()).or_insert(0)+=1;
        *maps.op_ret_size.entry(parsed.script_size).or_insert(0)+=1;

        if op_ret_proto.starts_with("0004") {
            *maps.veriblock_per_month.entry(parsed.ym.clone()).or_insert(0) += 1;
        }
    }

    if parsed.is_last_month {
        *maps.tx_per_template_last_month.entry(parsed.script.clone()).or_insert(0)+=1;
    }

    *maps.txo_size.entry(parsed.script_size).or_insert(0)+=1;
    *maps.txo_per_month.entry(parsed.ym).or_insert(0)+=1;
    *maps.tx_per_template.entry(parsed.script).or_insert(0)+=1;


}

quick_main!(run);