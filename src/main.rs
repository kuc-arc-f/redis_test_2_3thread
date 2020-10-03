// Rust + redis 書込み。3 thread並列
//
extern crate redis;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use redis::{Commands};
use serde::{Deserialize, Serialize};

const KEY_NAME : &str ="list_3";
//
pub fn get_content(filename: String ) -> String{
//    println!("In file {}", filename);
    let mut f = File::open(filename).expect("file not found");

    let mut contents = String::new();
    f.read_to_string(&mut contents)
        .expect("something went wrong reading the file");

//    println!("With text:\n{}", contents);
    return contents;
}
//
#[derive(Serialize, Deserialize , Debug)]
struct TaskItem {
    id: i64,
    title: String,
    content: String,
} 
//
fn create_thread( items: Vec<TaskItem> ) -> JoinHandle<()>{
    let handle = thread::spawn( move|| {
        let client = redis::Client::open("redis://localhost/").expect("url error");
        let mut connection = client.get_connection().expect("connect error");
        for row in &items {
            let serialized = serde_json::to_string(&row ).unwrap();
            let result2: u8 = connection.rpush(KEY_NAME, &serialized ).unwrap();
        }
    });
    return handle;
}
//
fn conver_array_3(items: Vec<TaskItem>, thread_num : i64)
     ->(Vec<TaskItem>,Vec<TaskItem>, Vec<TaskItem>){
    let mut count: usize = 1;
    let size = &items.len();
    let thread_num_big = thread_num as usize;
    let n1_max = size / thread_num_big;
    let n2_max = n1_max * 2;

//    println!("n1={}", n1 );
    let mut items_1 : Vec<TaskItem> = Vec::new();
    let mut items_2 : Vec<TaskItem> = Vec::new();
    let mut items_3 : Vec<TaskItem> = Vec::new();    
    for item in &items {
        let d = TaskItem { 
            id: item.id ,
            title: String::from(&item.title), 
            content: String::from(&item.content) 
        };
        if(count <= n1_max){
            items_1.push( d );
        }else if((count > n1_max) && (count <= n2_max) ){
            items_2.push( d );
        }else{
            items_3.push( d );
        }
        count += 1;
    } 
    return (items_1, items_2, items_3)   
}
//
fn main() {
    println!("#start");
    let fname = "/home/naka/work/node/express/app7/public/tasks.json";
    let json = get_content( fname.to_string() );
    let deserialized: Vec<TaskItem> = serde_json::from_str(&json).unwrap();
    let tup = conver_array_3( deserialized ,3);
//    println!("len1={}" , tup.0.len() );
    let handle = create_thread( tup.0 );
    let handle_2 = create_thread(tup.1 );
    let handle_3 = create_thread(tup.2 );

    handle.join().unwrap();
    handle_2.join().unwrap();
    handle_3.join().unwrap();
}
