NODE_NUM=$1

RemoteUser=root
LOCAL_HOST="10.32.0.1"

WORKER_HOSTS_2="
"10.32.0.1"
"10.44.0.0"
"
WORKER_HOSTS_4="
"10.35.0.0"
"10.40.0.0"
"10.32.0.1"
"10.44.0.0"
"
WORKER_HOSTS_6="
"10.35.0.0"
"10.40.0.0"
"10.42.0.0"
"10.36.0.0"
"10.32.0.1"
"10.44.0.0"
"

start_on_local() {
	echo "Start Localhost..."
	/opt/spark/sbin/start-worker.sh spark://9fcebbf32068:7077
	echo "Done local"
}

start_remote() {
	echo "Starting worker on Host: $1..."
	ssh -o StrictHostKeyChecking=no ${RemoteUser}@$1 "/opt/spark/sbin/start-worker.sh spark://9fcebbf32068:7077"
	echo "Done"
}

if [ $NODE_NUM -eq "2" ]; then
	for host in $WORKER_HOSTS_2; do
		if [[ $host == $LOCAL_HOST ]]; then
			start_on_local
		else
			start_remote $host
		fi
		sleep 8
	done
elif [ $NODE_NUM -eq "4" ]; then
	for host in $WORKER_HOSTS_4; do
		if [[ $host == $LOCAL_HOST ]]; then
			start_on_local
		else
			start_remote $host
		fi
		sleep 8
	done
elif [ $NODE_NUM -eq "6" ]; then
	for host in $WORKER_HOSTS_6; do
		if [[ $host == $LOCAL_HOST ]]; then
			start_on_local
		else
			start_remote $host
		fi
		sleep 8
	done
fi

echo ""
echo "All Done"
