#!/usr/bin/env bash

if [ $# != 4 ];
then
 echo "Please run script like ./generateServiceCatalog.sh <etcd username> <etcd password> <etcd host> <etcd port>"
 exit 1
fi

#Set etcd parameters
etcdUsername=$1
etcdUserpwd=$2
etcdHost=$3
etcdPort=$4

#hdfs
curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/metadata -XPUT -d value='{"longDescription":"Hadoop分布式文件系统(HDFS)是一个的分布式的，可扩展的，轻量级的文件系统。","documentationUrl":"http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html","providerDisplayName":"Asiainfo","displayName":"HDFS","imageUrl":"https://hadoop.apache.org/images/hadoop-logo.jpg","supportUrl":"http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html"}'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/name -XPUT -d value='HDFS'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/description -XPUT -d value='HDFS是Hadoop的分布式文件系统。版本：v2.7.1'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/bindable -XPUT -d value='true'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/planupdatable -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/tags -XPUT -d value='hdfs,storage'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan/72150b09-1025-4533-8bae-0e04ef68ac13 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan/72150b09-1025-4533-8bae-0e04ef68ac13/description -XPUT -d value='共享HDFS实例'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan/72150b09-1025-4533-8bae-0e04ef68ac13/free -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan/72150b09-1025-4533-8bae-0e04ef68ac13/name -XPUT -d value='shared'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae67d4ba-5c4e-4937-a68b-5b47cfe356d8/plan/72150b09-1025-4533-8bae-0e04ef68ac13/metadata -XPUT -d value='{"costs":[{"amount":{"usd":0.0},"unit":"MONTHLY"}],"bullets":["Name Space Quota:1000", "Storage Space Quota (GB):20"], "customize":{"nameSpaceQuota": {"default":1000,"max":100000,"price":10,"unit":"","step":10,"desc":"HDFS目录允许创建的最大文件数目"},“storageSpaceQuota”:{"default":1024,"max":102400,"price":10,"unit":"GB","step":10,"desc":"HDFS目录的最大存储容量"}}}'

# hbase
curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/metadata -XPUT -d value='{"longDescription":"HBase是一个开源的，非关系型的，分布式数据库，类似于Google的BigTable。","documentationUrl":"http://hbase.apache.org/","providerDisplayName":"Asiainfo","displayName":"HBase","imageUrl":"http://hbase.apache.org/images/hbase_logo_with_orca_large.png","supportUrl":"http://hbase.apache.org/book.html"}'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/name -XPUT -d value='HBase'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/description -XPUT -d value='HBase是Hadoop的面向列的分布式非关系型数据库。版本：v1.1.2'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/bindable -XPUT -d value='true'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/planupdatable -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/tags  -XPUT -d value='hbase,database'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan/f658e391-b7d6-4b72-9e4c-c754e4943ae1 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan/f658e391-b7d6-4b72-9e4c-c754e4943ae1/description -XPUT -d value='共享HBase实例'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan/f658e391-b7d6-4b72-9e4c-c754e4943ae1/free -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan/f658e391-b7d6-4b72-9e4c-c754e4943ae1/name -XPUT -d value='shared' 

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d9845ade-9410-4c7f-8689-4e032c1a8450/plan/f658e391-b7d6-4b72-9e4c-c754e4943ae1/metadata -XPUT -d value='{"costs":[{"amount":{"usd":0.0},"unit":"MONTHLY"}],"bullets":["HBase Maximun Tables:10", "HBase Maximun Regions:10"], "customize":{"maximumTablesQuota":{"default":10,"max":100,"price":10,"unit":"","step":10,"desc":"HBase命名空间允许的最大的表数目"}, "maximumRegionsQuota":{"default":100,"max":1000,"price":10,"unit":"","step":10,"desc":"HBase命名空间允许的最大的region数目"}}}'

#hive
curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/metadata -XPUT -d value='{"longDescription":"Hive是一个可以通过SQL去读写，管理存储在分布式存储系统上的大规模数据集的数据仓库解决方案。","documentationUrl":"http://hive.apache.org/","providerDisplayName":"Asiainfo","displayName":"Hive","imageUrl":"https://hive.apache.org/images/hive_logo_medium.jpg","supportUrl":"https://cwiki.apache.org/confluence/display/Hive/Home#Home-UserDocumentation"}'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/name -XPUT -d value='Hive'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/description -XPUT -d value='Hive是Hadoop的数据仓库。版本：v1.2.1'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/bindable -XPUT -d value='true'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/planupdatable -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/tags  -XPUT -d value='hive,datawarehouse'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan/aa7e364f-fdbf-4187-b60a-218b6fa398ed -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan/aa7e364f-fdbf-4187-b60a-218b6fa398ed/description -XPUT -d value='共享Hive实例'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan/aa7e364f-fdbf-4187-b60a-218b6fa398ed/free -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan/aa7e364f-fdbf-4187-b60a-218b6fa398ed/name -XPUT -d value='shared' 

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/2ef26018-003d-4b2b-b786-0481d4ee9fa3/plan/aa7e364f-fdbf-4187-b60a-218b6fa398ed/metadata -XPUT -d value='{"costs":[{"amount":{"usd":0.0},"unit":"MONTHLY"}],"bullets":["Shared Hive Server (GB):20", "Yarn Queue Quota (GB):4"], "customize":{"storageSpaceQuota": {"default":1024,"max":102400,"price":10,"unit":"GB","step":10,"desc":"Hive数据库的最大存储容量"}, "yarnQueueQuota":{"default":10,"max":100,"price":10,"unit":"GB","step":10,"desc":"Yarn队列的最大容量"}}}'

#mr2
curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/metadata -XPUT -d value='{"longDescription":"Hadoop MapReduce是一个可以快速编写能够在大规模集群(数千节点)上处理大规模数据(TB级)的可靠的，容错的应用的软件框架。","documentationUrl":"http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html ","providerDisplayName":"Asiainfo","displayName":"MapReduce","imageUrl":"https://hadoop.apache.org/images/hadoop-logo.jpg","supportUrl":"http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html"}'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/name -XPUT -d value='MapReduce'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/description -XPUT -d value='MapReduce是Hadoop的分布式计算框架。版本：v2.7.1'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/bindable -XPUT -d value='true'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/planupdatable -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/tags  -XPUT -d value='MapReduce,compute engine'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan/6524c793-0ea5-4456-9a60-ca70271decdc -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan/6524c793-0ea5-4456-9a60-ca70271decdc/description -XPUT -d value='共享MapReduce实例'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan/6524c793-0ea5-4456-9a60-ca70271decdc/free -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan/6524c793-0ea5-4456-9a60-ca70271decdc/name -XPUT -d value='shared'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/ae0f2324-27a8-415b-9c7f-64ab6cd88d40/plan/6524c793-0ea5-4456-9a60-ca70271decdc/metadata -XPUT -d value='{"costs":[{"amount":{"usd":0.0},"unit":"MONTHLY"}],"bullets":["Yarn Queue Quota (GB):4"], "customize":{"yarnQueueQuota":{"default":10,"max":100,"price":10,"unit":"GB","step":10,"desc":"Yarn队列的最大容量"}}}'

#spark
curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/metadata -XPUT -d value='{"longDescription":"Apache Spark是一个快速的，通用性的集群计算系统。","documentationUrl":"http://spark.apache.org/","providerDisplayName":"Asiainfo","displayName":"Spark","imageUrl":"https://spark.apache.org/images/spark-logo.png","supportUrl":"http://spark.apache.org/docs/1.6.0/"}'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/name -XPUT -d value='Spark'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/description -XPUT -d value='Spark是一种的通用并行计算框架。版本：v1.6.0'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/bindable -XPUT -d value='true'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/planupdatable -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/tags  -XPUT -d value='spark,compute engine'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan/5c3d471d-f94a-4bb8-b340-f783f3c15ba1 -XPUT -d dir=true

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan/5c3d471d-f94a-4bb8-b340-f783f3c15ba1/description -XPUT -d value='共享Spark实例'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan/5c3d471d-f94a-4bb8-b340-f783f3c15ba1/free -XPUT -d value='false'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan/5c3d471d-f94a-4bb8-b340-f783f3c15ba1/name -XPUT -d value='shared'

curl http://$etcdUsername:$etcdUserpwd@$etcdHost:$etcdPort/v2/keys/servicebroker/ocdp/catalog/d3b9a485-f038-4605-9b9b-29792f5c61d1/plan/5c3d471d-f94a-4bb8-b340-f783f3c15ba1/metadata -XPUT -d value='{"costs":[{"amount":{"usd":0.0},"unit":"MONTHLY"}],"bullets":["Yarn Queue Quota (GB):4"],"customize":{"yarnQueueQuota":{"default":10,"max":100,"price":10,"unit":"GB","step":10,"desc":"Yarn队列的最大容量"}}}'
