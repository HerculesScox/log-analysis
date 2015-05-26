# log-analysis
A Hive log anaylze tool on hadoop2.0 clusters.
When HQL be executed on hadoop, some log files are generated in local disk(default). This tool analyze log fils by the aid of yarn which log aggregation function, that is to say all log files from directory which be set by following parameters in HDFS：
  mapreduce.jobhistory.done-dir
  yarn.node.manager.remote-app-log-dir

This tool could establish a relation between query, job and task when anaylzing log information, persistence data into database. Another a part is visualization.

Requirements：
	jdk 1.8;
	hadoop 2.0;
	hive 0.12+;
	
