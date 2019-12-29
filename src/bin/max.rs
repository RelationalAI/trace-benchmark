#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::time::Instant;

use rand::distributions::{Distribution, Uniform};

use differential_dataflow::input::Input;
use differential_dataflow::operators::Reduce;

use itertools::Itertools;

type K = u64;
type V = u64;
type T = u64;
type R = isize;

#[derive(Debug, Deserialize)]
struct Config {
    distinct_keys: u64,
    rounds: u64,
    round_size: u64,
    fixpoint_depth: u64,
}

impl Config {
    fn path(&self, base: &str) -> String {
        format!("{}/{}_keys_{}_per_round.csv", base, self.distinct_keys, self.round_size)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Configurations {
    experiment: String,
    distinct_keys: Vec<u64>,
    rounds: u64,
    round_size: Vec<u64>,
    fixpoint_depth: u64,
}

impl Configurations {
    fn path(&self) -> String {
        format!("measurements/{}", self.experiment)
    }
    
    fn read(path: &str) -> Self {
        let mut file = File::open(path).expect("config file not found");

        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("failed to read config file");

        toml::from_str::<Self>(&contents).expect("failed to parse config file")
    }
}

fn main() {
    let config_path: String = std::env::args().nth(1).unwrap().parse().unwrap();
    let configurations: Configurations = Configurations::read(&config_path);

    let config_iter = configurations.distinct_keys.iter().cloned()
        .cartesian_product(configurations.round_size.iter().cloned());

    for (distinct_keys, round_size) in config_iter.into_iter() {

        let base = configurations.path();
        let rounds = configurations.rounds.clone();
        let fixpoint_depth = configurations.fixpoint_depth.clone();
        
        timely::execute_from_args(std::env::args().skip(2), move |worker| {

            let config = Config {
                distinct_keys,
                round_size,
                rounds,
                fixpoint_depth,
            };

            println!("{:?}", config);
            
            let mut out = BufWriter::new(File::create(config.path(&base))
                                         .expect("failed to create output file"));

            let mut rng = rand::thread_rng();

            let key_dist = Uniform::from(0..config.distinct_keys);
            let value_dist = Uniform::from(0..1_000_000);

            let mut start = Instant::now();

            let (mut input, probe) = worker.dataflow::<T, _, _>(|scope| {
                let (input, data) = scope.new_collection::<_, R>();

                let probe = data
                    .reduce(|_k, vals: &[(&V, isize)], output| {
                        let max: &V = &vals[vals.len() - 1].0;
                        output.push((max.clone(), 1));
                    })
                // .inspect(|x| println!("{:?}", x))
                    .probe();
                
                (input, probe)
            });

            for t in 0 .. config.rounds {
                for i in 0 .. config.round_size {
                    let k = key_dist.sample(&mut rng);
                    let v = value_dist.sample(&mut rng);
                    let diff = 1;

                    input.update((k, v), diff);
                }

                input.advance_to(t + 1);
                input.flush();

                worker.step_while(|| probe.less_than(input.time()));

                if input.time() % 10 == 0 {
                    // println!("{} {}", input.time(), start.elapsed().as_nanos());
                    write!(out, "{},{}\n", input.time(), start.elapsed().as_nanos())
                        .expect("failed to write output");
                }

                start = Instant::now();
            }
        }).unwrap();
    }
}
