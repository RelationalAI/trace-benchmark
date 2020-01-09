use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use timely::dataflow::operators::probe::Probe;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;

type K = ();
type V = ();
type T = u64;
type R = isize;

fn main() {
    let input_path: String = std::env::args().nth(1).unwrap().parse()
        .expect("no input path specified");
    let batch_size: usize = std::env::args().nth(2).unwrap().parse()
        .expect("no batch size specified"); 

    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let (mut input, probe) = worker.dataflow::<T, _, _>(|scope| {
            let (input, data) = scope.new_collection::<_, R>();

            let probe = data
                // .inspect(|x| println!("{:?}", x))
                .arrange_by_key()
                .stream
                .probe();
            
            (input, probe)
        });

        let measurements = File::open(&input_path)
            .expect("failed to open measurements file");

        let mut reader = BufReader::new(measurements);

        let mut start = Instant::now();

        let mut buf = String::new();
        let mut i = 0;

        loop {
            match reader.read_line(&mut buf) {
                Ok(0) => { break; }
                Ok(_n) => {
                    if buf.ends_with("\n") {
                        buf.pop();
                    }

                    let r = buf.parse::<R>()
                        .expect("failed to parse measurement");
                    
                    input.update(((), ()), r);

                    if i % batch_size == 0 {
                        // println!("{} {} {}", i, buf, start.elapsed().as_nanos());
                        // write!(out, "{},{}\n", input.time(), start.elapsed().as_nanos())
                        //     .expect("failed to write output");
                        input.advance_to((i + 1) as u64);
                        input.flush();
                        
                        worker.step_while(|| probe.less_than(input.time()));
                    }

                    start = Instant::now();
                    i += 1;
                    buf.clear();
                }
                Err(_e) => { panic!("fail"); }
            }
        }

        input.close();

        worker.step_while(|| !probe.done());
    }).unwrap();
}
