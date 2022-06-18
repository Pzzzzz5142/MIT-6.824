runs=$1
unix_ts="$(date +%s)"
tmp="$unix_ts"_tmp
Test=$2
mkdir $tmp
cp * $tmp
cd $tmp
for i in $(seq 1 $runs); do
    ret=0
    go test -run $Test || ret=$?
    pid=$!
    if [ $ret -ne 0 ]; then
        echo '***' FAILED TESTS IN TRIAL $i 'dir' $tmp
        exit 1
    fi
done
cd ..
rm -rf $tmp