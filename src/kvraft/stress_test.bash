#!/bin/bash

set -e
set -o pipefail
trap ctrl_c INT

function ctrl_c()
{
    echo "Trap: CTRL+C received, exit"
    exit
}

rm -r ./test_out
mkdir test_out

for i in $(seq 0 31)
do
    echo "running $(expr $i \* 8)~$(expr $i \* 8 + 7)"
    # timeout --signal=SIGINT 300 ./kvraft.test -test.run 3B 2>test_out/$(expr $i \* 8 + 0).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 9 + 0).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 1).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 2).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 3).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 4).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 5).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 6).test &
    ./kvraft.test -test.run 3B 2>&1 | tee test_out/$(expr $i \* 8 + 7).test
done

echo "no error! congrats!"