set term pdf enhanced color linewidth 2 font "Helvetica,14";
set grid;

set key box;
set key inside left top;
set key spacing 1.45;

set datafile separator ",";

set title '{/Helvetica-Bold MAX MONOID Aggregate (1 update per round)}';
set xlabel 'Round';
set ylabel 'Latency (ns)';

set output "max_monoid_1_per_round.pdf"

plot '../measurements/max_monoid/1_keys_1_per_round.csv' using 1:2 title '1 key' with lines,\
     '../measurements/max_monoid/10_keys_1_per_round.csv' using 1:2 title '10 keys' with lines,\
     '../measurements/max_monoid/1000_keys_1_per_round.csv' using 1:2 title '1k keys' with lines,\
     '../measurements/max_monoid/10000_keys_1_per_round.csv' using 1:2 title '10k keys' with lines,\

set output "max_monoid_1000_per_round.pdf"
set title '{/Helvetica-Bold MAX MONOID Aggregate (1k updates per round)}';

plot '../measurements/max_monoid/1_keys_1000_per_round.csv' using 1:2 title '1 key' with lines,\
     '../measurements/max_monoid/10_keys_1000_per_round.csv' using 1:2 title '10 keys' with lines,\
     '../measurements/max_monoid/1000_keys_1000_per_round.csv' using 1:2 title '1k keys' with lines,\
     '../measurements/max_monoid/10000_keys_1000_per_round.csv' using 1:2 title '10k keys' with lines,\
