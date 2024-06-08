
TIGER_LIST="
7
8
2
6
3
4
"
#1 - cannot stop & excluded from paper
#5 - excluded

OSM_LIST="
10
11
"
#12 - 2nd largest
#9  - 1st largest

################
# TIGER 2 Nodes
################

# SPARK Config
# /home/hadoop/deploy/spark_stop_workers.sh 4 # Previous run was 4-Nodes, so use "4" here
# sleep 3
# /opt/spark/sbin/stop-master.sh
# sleep 10
# cp /opt/spark/conf/workers-2 /opt/spark/conf/workers
# /opt/spark/sbin/start-master.sh
# sleep 5
# /home/hadoop/deploy/spark_start_workers.sh 2
# sleep 5

# for queryNum in $TIGER_LIST; do
# 	echo "Running Query [$queryNum]..."
# 	/opt/spark/bin/spark-submit --driver-memory 6g --executor-memory 8g target/sedona-spark-example-1.6.0.jar $queryNum >log-$queryNum-2 2>&1
# 	echo "Query done"
# done
# echo "Tiger Done"

#################
# OSM 2-4-6 Nodes
#################

##### 6 Nodes ####9

# SPARK Config
/home/hadoop/deploy/spark_stop_workers.sh 2 # Previous run was 4-Nodes, so use "4" here
sleep 5
res=`/opt/spark/sbin/stop-master.sh`
echo "Stop master exiting: $res"
cp /opt/spark/conf/workers-6 /opt/spark/conf/workers
sleep 10
res=`/opt/spark/sbin/start-master.sh`
echo "Start master exiting: $res"
sleep 5
/home/hadoop/deploy/spark_start_workers.sh 6
sleep 5

# Run Query
for queryNum in $OSM_LIST; do
	echo "Running Query [$queryNum]..."
	/opt/spark/bin/spark-submit --driver-memory 6g --executor-memory 8g target/sedona-spark-example-1.6.0.jar $queryNum >log-$queryNum-6 2>&1
	echo "Query done"
	sleep 10
done


##### 4 Nodes ####

# SPARK Config
/home/hadoop/deploy/spark_stop_workers.sh 6 # Previous run was 4-Nodes, so use "4" here
sleep 5
res=`/opt/spark/sbin/stop-master.sh`
echo "Stop master exiting: $res"
cp /opt/spark/conf/workers-4 /opt/spark/conf/workers
sleep 10
res=`/opt/spark/sbin/start-master.sh`
echo "Start master exiting: $res"
sleep 5
/home/hadoop/deploy/spark_start_workers.sh 4
sleep 5

# Run Query
for queryNum in $OSM_LIST; do
	echo "Running Query [$queryNum]..."
	/opt/spark/bin/spark-submit --driver-memory 6g --executor-memory 8g target/sedona-spark-example-1.6.0.jar $queryNum >log-$queryNum-4 2>&1
	echo "Query done"
	sleep 10
done


##### 2 Nodes ####

# SPARK Config
/home/hadoop/deploy/spark_stop_workers.sh 4 # Previous run was 4-Nodes, so use "4" here
sleep 5
res=`/opt/spark/sbin/stop-master.sh`
echo "Stop master exiting: $res"
cp /opt/spark/conf/workers-2 /opt/spark/conf/workers
sleep 10
res=`/opt/spark/sbin/start-master.sh`
echo "Start master exiting: $res"
sleep 5
/home/hadoop/deploy/spark_start_workers.sh 2
sleep 5

# Run Query
for queryNum in $OSM_LIST; do
	echo "Running Query [$queryNum]..."
	/opt/spark/bin/spark-submit --driver-memory 6g --executor-memory 8g target/sedona-spark-example-1.6.0.jar $queryNum >log-$queryNum-2 2>&1
	echo "Query done"
	sleep 10
done

echo "ALL TESTS DONE"
