runs=$1
unix_ts="$(date +%s%3)"
tmp="$unix_ts"_tmp
mkdir $tmp
cp * $tmp
cd $tmp
for i in $(seq 1 $runs); do
    ret=0
    go test -run 2A -race || ret=$?
    pid=$!
    if [ $ret -ne 0 ]; then
        echo '***' FAILED TESTS IN TRIAL $i
        cd ..
        rm -rf $tmp
        exit 1
    fi
done
cd ..
rm -rf $tmp