NODE_NUM=$1

RemoteUser=root
MASTER_HOST="10.32.0.1"

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

stop_on_local() {
        echo "Stopping Localhost..."
        /opt/spark/sbin/stop-worker.sh
        echo "Done local"
}

stop_remote() {
	echo "Stopping worker on Host: $1..."
	ssh -o StrictHostKeyChecking=no ${RemoteUser}@$1 "/opt/spark/sbin/stop-worker.sh"
	echo "Done"
}

if [ $NODE_NUM -eq "2" ]; then
	for host in $WORKER_HOSTS_2; do
		if [[ $host == $MASTER_HOST ]]; then
			stop_on_local
		else
			stop_remote $host
		fi
		sleep 2
	done
elif [ $NODE_NUM -eq "4" ]; then
	for host in $WORKER_HOSTS_4; do
		if [[ $host == $MASTER_HOST ]]; then
			stop_on_local
		else
			stop_remote $host
		fi
		sleep 2
	done
elif [ $NODE_NUM -eq "6" ]; then
	for host in $WORKER_HOSTS_6; do
		if [[ $host == $MASTER_HOST ]]; then
			stop_on_local
		else
			stop_remote $host
		fi
		sleep 2
	done
fi

echo ""
echo "All Done"
