use std::{env, ffi::OsStr};
use std::fs::{File, read};
use std::io::{BufReader, Lines};
use serde::{Deserialize, Serialize};
use flate2::read::GzDecoder;
use std::io::BufRead;
use std::time::Instant;
use arrow::datatypes::ToByteSlice;
use arrow::ipc::Bool;
use prost::Message;
use serde_json::{Value, Map};
use rayon::prelude::*;


extern crate redis;

pub mod snazzy {
    pub mod items {
        include!(concat!(env!("OUT_DIR"), "/snazzy.items.rs"));
    }
}

pub mod chalk {
    pub mod protos {
        include!(concat!(env!("OUT_DIR"), "/chalk.protos.rs"));
    }
}


use redis::{Commands, Connection, from_redis_value, Pipeline, RedisError, RedisResult, RedisWrite, ToRedisArgs};

#[derive(Debug,  Deserialize, Serialize)]
struct LegacyStoredValue {
    encoded_val: String,
}

fn read_file(filename: &str) -> std::iter::Map<Lines<BufReader<GzDecoder<File>>>, fn(std::io::Result<String>) -> Value> {
    let mut reader = BufReader::new(GzDecoder::new(File::open(filename).unwrap()));

    let mut buffer = String::new();
    let mut i: i64 = 0;

    return reader.lines().map(|line| serde_json::from_str::<Value>(&line.expect("got a line")).expect("Failed parse"))



    //     while(true) {
    //     i += 1;
    //     buffer.clear();
    //     let len = reader.lines(&mut buffer).unwrap();
    //     // let parsed: Value = serde_json::from_str(buffer.as_str()).unwrap();
    //     // let obj: &Map<String, Value> = parsed.as_object().unwrap();
    //     if len == 0 {
    //         break
    //     }
    //
    //     if i % 1000 == 0 {
    //         println!("{}", i)
    //     }
    //
    // }
}

struct RedisValue {
    __id__: String,
    __ns__: String,
    __ts__: i64
}

fn v_to_proto(p0: &Value) -> chalk::protos::Value {
    let obj = p0.as_object().expect("must be obj");

    let inner = if (obj.contains_key("S")) {
        Some(chalk::protos::value::Val::StringVal(obj.get("S").expect("S").as_str().expect("non-empty-str").to_string()))
    } else if (obj.contains_key("N")) {
        Some(chalk::protos::value::Val::DoubleVal(obj.get("N").expect("N").as_str().expect("str in N").to_string().parse::<f64>().expect(&format!("non-empty-int: {:?}", obj))))
    } else if (obj.contains_key("NULL")) {
        Some(chalk::protos::value::Val::NullVal(0))
    } else {
        panic!("Not sure what to do with {:?}", obj)
    };

    let mut ret = chalk::protos::Value::default();

    ret.val = inner;

    return ret
}

impl ToRedisArgs for chalk::protos::Value {
    fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
        use prost::Message;
        out.write_arg(&self.encode_to_vec())
    }
}

#[tokio::main]
async fn main() {
    println!("Opening redis connection...");


    let client = redis::Client::open().expect("redis cli");

    let mut con = client.get_connection().expect("redis connection should be work");
    println!("Testing redis...");
    let x: RedisResult<i64> = con.exists("test");
    println!("{:?}", x);


    let BATCH_SIZE = 100;

    // let paths = std::fs::read_dir("/Users/andrew/Downloads").unwrap().filter(|f| f.unwrap().file_name() == ".json.gz")
    let paths: Vec<_> = vec![
            "/Users/andrew/Downloads/2223fggxxez4xhksgcsxdb35mi.json.gz",
            "/Users/andrew/Downloads/222lels47e6dzo7ozwnwoblpoy.json.gz",
            "/Users/andrew/Downloads/22m3w4ulli245hncczmn4uaiiq.json.gz",
            "/Users/andrew/Downloads/23s45dldpq3e3kl4xzlmchudba.json.gz",
            "/Users/andrew/Downloads/24vkur6iayyfxjxviw4psg7iam.json.gz",
            "/Users/andrew/Downloads/tfnzwfq2pa7odk5d4yi2wsqfw4.json.gz",
    ];

    paths.into_par_iter().for_each(|file| process_file(BATCH_SIZE, file));

    // let client = redis::Client::open("redis://:6284001732@127.0.0.1").unwrap();
    //
    // let mut con2 = client.get_connection().unwrap();



    // let iter : redis::Iter<String> = redis::cmd("SCAN")
    //     .cursor_arg(0).clone().iter(&mut con).unwrap();
    //
    // let mut i = 0;
    // for x in iter {
    //     i += 1;
    //
    //     if i % 100 == 0 {
    //         println!("i: {}, {}", i, x)
    //     }
    //
    //     let val: RedisResult<String> = con2.get(x);
    //
    //     let deser = serde_json::from_str::<LegacyStoredValue>(&val.unwrap()).unwrap();
    //     println!("{:?}", deser);
    //
    //
    // }

    // println!("Hello!")

}

fn process_file(BATCH_SIZE: i32, file: &str) {
    println!("Processing file: {}", file);
    let client = redis::Client::open(").expect("redis cli");
    let mut con = client.get_connection().expect("redis connection should be work");
    let mut pipe = redis::pipe();

    let mut i = 0;
    let mut start = std::time::Instant::now();
    for x in read_file(file).take(100000) {
        let item_as_object = x.as_object().expect("Should be a json object").get("Item").expect("something").as_object().expect("object");
        let mut id = item_as_object.get("__id__").expect("__id__").as_object().expect("must have obj").get("S").expect(&format!("non-empty id in {:?}", item_as_object)).as_str().expect("must be str");
        let mut ns = item_as_object.get("__ns__").expect("__ns__").as_object().expect("must have obj").get("S").expect(&format!("non-empty ns in {:?}", item_as_object)).as_str().expect("must be str");
        let mut ts = item_as_object.get("__ts__").expect("__ts__").as_object().expect("must have obj").get("N").expect(&format!("non-empty ts in {:?}", item_as_object)).as_str().expect("must be str").parse::<i64>().expect("to i64");

        let mut keys: Vec<(&str, Vec<u8>)> = Vec::with_capacity(item_as_object.len() - 3);

        for (k, v) in item_as_object.into_iter() {
            let tuple: (&str, Vec<u8>) = (k.as_str(), v_to_proto(v).encode_to_vec());
            keys.push(tuple);
        }

        let k = format!("{ns}:{id}");
        // let r_keys: &[(&str, &[u8])] = &keys;
        pipe.hset_multiple(k, &keys);
        i += 1;

        if i % BATCH_SIZE == 0 {
            println!("{} :: i: {} -> redis @ {:.2?}ms", file, i, start.elapsed().as_millis());
            let res: RedisResult<String> = pipe.query(&mut con);
            pipe.clear();
            println!("{} :: i: {} {:.2?}ms", file, i, start.elapsed().as_millis());
            start = std::time::Instant::now();
        }
    }
}

