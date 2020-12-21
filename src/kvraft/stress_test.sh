set -e

for i in $(seq 1 100)
do
    echo "running $i"
    timeout --signal=SIGINT 3 ./kvraft.test -test.run PersistPartitionUnreliable3A 2>test_out/$i.test
done

echo "no error! congrats!"