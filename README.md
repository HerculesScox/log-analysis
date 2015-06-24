log-analysis
===
A Hive log anaylze tool on hadoop2.0 clusters.

When HQL be executed on hadoop, some log files are generated in local disk(default). This tool analyze log fils by the aid of yarn which log aggregation function, that is to say all log files from directory which be set by following parameters in HDFS：
>>mapreduce.jobhistory.done-dir</br>
>>yarn.node.manager.remote-app-log-dir

This tool could establish a relation between query, job and task when anaylzing log information, persistence data into database. Another a part is visualization.

![job_dp](https://cloud.githubusercontent.com/assets/4024711/8325309/f940eed2-1a8a-11e5-9fb6-8def365ec1fd.png)
![time](https://cloud.githubusercontent.com/assets/4024711/8325312/011fa670-1a8b-11e5-9f0f-64b7c431b9f6.png)
![map](https://cloud.githubusercontent.com/assets/4024711/8325313/023c71d2-1a8b-11e5-9925-6a2df842a01a.png)
<h4>Requirements：</h4>
- jdk 1.8;
- hadoop 2.0;
- hive 0.12+;
	
