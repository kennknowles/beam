use std::collections::HashMap;

fn plus_one(x: i32) -> i32 {
    x + 1
}

fn plus_two(x: i32) -> i32 {
    x + 2
}

fn times_two(x: i32) -> i32 {
    x * 2
}

type DoFn = fn(i32) -> i32;

fn get_dofns() -> HashMap<String, DoFn> {
    let mut dofns: HashMap<String, DoFn> = HashMap::new();
    dofns.insert("plus_one".to_string(), plus_one);
    dofns.insert("plus_two".to_string(), plus_two);
    dofns.insert("times_two".to_string(), times_two);
    dofns
}

fn main() {
    let graph = vec!["times_two", "plus_one", "plus_two", "times_two"];

    let mut pcollection = vec![1i32, 2, 3];
    println!("Initial pcollection: {:?}", pcollection);
    
    for x in pcollection.iter_mut() {
        for dofn_name in graph.iter() {
            *x = get_dofns()[&dofn_name.to_string()](*x);
        }
    }
    
    println!("Final pcollection: {:?}", pcollection);
}
