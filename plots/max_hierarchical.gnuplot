set term pdf enhanced color linewidth 2 font "Helvetica,14";
set grid;

set datafile separator ",";

set title '{/Helvetica-Bold Hierarchical MAX Aggregate (1 update per round)}';
set xlabel 'Round';
set ylabel 'Latency (ns)';

set yrange [0:20000000];

set output "max_hierarchical_1_per_round.pdf"

plot '../measurements/max_hierarchical/1_keys_1_per_round.csv' using 1:2 title '1 key' with lines

set output "max_hierarchical_1000_per_round.pdf"
set title '{/Helvetica-Bold Hierarchical MAX Aggregate (1k updates per round)}';

plot '../measurements/max_hierarchical/1_keys_1000_per_round.csv' using 1:2 title '1 key' with lines
