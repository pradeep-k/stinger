(time ./stinger_gbench -v 2097152 -i ~/data/kron-21/bin/ -s 1 -t 19 -a 1) > kr21_u.txt 2>&1
(time ./stinger_gbench -v 2097152 -i ~/data/kron-21/mix_bins/ -s 1 -t 19 -a 1) > kr21_del_u.txt 2>&1
#(time ./stinger_gbench -i ~/data/twitter_mpi/bin/ -v 52579682 -t 19 -b 65536 -s 1 -a 1) > tw_u.txt 2>&1
#(time ./stinger_gbench -i ~/data/twitter_mpi/mix_bins/ -v 52579682 -t 19 -b 65536 -s 1 -a 1) > tw_del_u.txt 2>&1
