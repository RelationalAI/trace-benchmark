use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::time::Instant;

use rand::distributions::{Distribution, Uniform};

use differential_dataflow::input::Input;
use differential_dataflow::operators::Reduce;
use differential_dataflow::operators::count::CountTotal;

use itertools::Itertools;

use trace_benchmark::{Config, Configurations};

type K = u64;
type V = u64;
type T = u64;
type R = isize;

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
                    .map(|key| ((key % 1000) as u16, key))
                    .reduce(|_k, s, t| {
                        let max = s.iter().map(|x| x.1).max().unwrap();
                        t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
                    })
                    // .inspect(|x| println!("{:?}", x))
                    .map(|(_, key)| ((key % 100) as u8, key))
                    .reduce(|_k, s, t| {
                        let max = s.iter().map(|x| x.1).max().unwrap();
                        t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
                    })
                    // .inspect(|x| println!("{:?}", x))
                    .map(|(_, key)| ((key % 10) as u8, key))
                    .reduce(|_k, s, t| {
                        let max = s.iter().map(|x| x.1).max().unwrap();
                        t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
                    })
                    // .inspect(|x| println!("{:?}", x))
                    .map(|(_, key)| ((), key))
                    .reduce(|_k, s, t| {
                        let max = s.iter().map(|x| x.1).max().unwrap();
                        t.extend(s.iter().filter(|x| x.1 == max).map(|&(&a,b)| (a,b)));
                    })
                    // .inspect(|x| println!("{:?}", x))
                    .map(|(_, key)| key)
                    .count_total()
                    .probe();

                (input, probe)
            });

            for t in 0..config.rounds {
                for i in 0..config.round_size {
                    let k = key_dist.sample(&mut rng);
                    // let v = value_dist.sample(&mut rng);
                    let diff = 1;

                    input.update(k, diff);
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
