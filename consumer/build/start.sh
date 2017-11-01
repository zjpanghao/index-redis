id=`pidof fetch_market_redis`
if test -n $id
then
kill -9 $id
echo "kill fetch_market_redis"
fi
echo "start"
nohup ./fetch_market_redis -k 192.168.1.74:9092 -t east_wealth &
