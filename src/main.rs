use std::collections::HashMap;

struct A {
    map2: HashMap<u32, HashMap<u32, u32>>,
}

fn main() {
    let mut m: HashMap<u32, &str> = HashMap::new();
    m.insert(1, "a");
    m.insert(2, "a");
    m.insert(3, "a");
    m.iter_mut()
        .map(|(_id, _)| _id.clone())
        .collect::<Vec<u32>>();
}
