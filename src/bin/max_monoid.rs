#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::time::Instant;

use rand::distributions::{Distribution, Uniform};

use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::input::Input;
use differential_dataflow::operators::Count;

use itertools::Itertools;

use trace_benchmark::{Config, Configurations};

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
struct MaxPlus {
    pub v: u64,
}

impl core::convert::From<u64> for MaxPlus {
    fn from(v: u64) -> Self {
        MaxPlus { v }
    }
}

impl<'a> core::ops::AddAssign<&'a Self> for MaxPlus {
    fn add_assign(&mut self, other: &'a Self) {
        *self = MaxPlus::from(core::cmp::max(self.v, other.v));
    }
}

impl Semigroup for MaxPlus {
    #[inline]
    fn is_zero(&self) -> bool {
        self.v == 0
    }
}

impl Monoid for MaxPlus {
    #[inline]
    fn zero() -> Self {
        MaxPlus { v: 0 }
    }
}

type T = u64;
type R = MaxPlus;

fn main() {
    let config_path: String = std::env::args().nth(1).unwrap().parse().unwrap();
    let configurations: Configurations = Configurations::read(&config_path);

    let config_iter = configurations
        .distinct_keys
        .iter()
        .cloned()
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

            let mut out = BufWriter::new(
                File::create(config.path(&base)).expect("failed to create output file"),
            );

            let mut rng = rand::thread_rng();

            let key_dist = Uniform::from(0..config.distinct_keys);
            let value_dist = Uniform::from(0..1_000_000);

            let mut start = Instant::now();

            let (mut input, probe) = worker.dataflow::<T, _, _>(|scope| {
                let (input, data) = scope.new_collection::<_, R>();

                let probe = data
                    .count()
                    // .inspect(|x| println!("{:?}", x))
                    .probe();

                (input, probe)
            });

            for t in 0..config.rounds {
                for i in 0..config.round_size {
                    let k = key_dist.sample(&mut rng);
                    let v = value_dist.sample(&mut rng);

                    input.update(k, MaxPlus::from(v));
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
        })
        .unwrap();
    }
}
