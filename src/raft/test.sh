runs=$1
for i in $(seq 1 $runs); do
    ret=0
    go test -run 2A -race || ret=$?
    pid=$!
    if [ $ret -ne 0 ]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done