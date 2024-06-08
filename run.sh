Q_LIST="
7
6
3
4
1
"
#2
#7
#6
#3
#5
#4
for queryNum in $Q_LIST; do
	echo "Running Query [$queryNum]..."
	/opt/spark/bin/spark-submit --driver-memory 6g --executor-memory 8g target/sedona-spark-example-1.6.0.jar $queryNum >log-$queryNum-4 2>&1
	echo "Query done"
done
echo "All Done"
