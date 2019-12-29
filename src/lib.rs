#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub distinct_keys: u64,
    pub rounds: u64,
    pub round_size: u64,
    pub fixpoint_depth: u64,
}

impl Config {
    pub fn path(&self, base: &str) -> String {
        format!("{}/{}_keys_{}_per_round.csv", base, self.distinct_keys, self.round_size)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Configurations {
    pub experiment: String,
    pub distinct_keys: Vec<u64>,
    pub rounds: u64,
    pub round_size: Vec<u64>,
    pub fixpoint_depth: u64,
}

impl Configurations {
    pub fn path(&self) -> String {
        format!("measurements/{}", self.experiment)
    }
    
    pub fn read(path: &str) -> Self {
        let mut file = File::open(path).expect("config file not found");

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("failed to read config file");

        toml::from_str::<Self>(&contents).expect("failed to parse config file")
    }
}
