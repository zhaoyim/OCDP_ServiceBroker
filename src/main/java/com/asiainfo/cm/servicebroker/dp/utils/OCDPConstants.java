package com.asiainfo.cm.servicebroker.dp.utils;

/**
 * Created by baikai on 6/22/17.
 */
public class OCDPConstants {

    public static final String HDFS_RESOURCE_TYPE = "HDFS_Path";

    public static final String HDFS_RANGER_RESOURCE_TYPE = "path";

    public static final String HBASE_RESOURCE_TYPE = "HBase_NameSpace";

    public static final String HBASE_RANGER_RESOURCE_TYPE = "table";

    public static final String HIVE_RESOURCE_TYPE = "Hive_Database";

    public static final String HIVE_RANGER_RESOURCE_TYPE = "database";

    public static final String MAPREDUCE_RESOURCE_TYPE = "Yarn_Queue";

    public static final String SPARK_RESOURCE_TYPE = "Yarn_Queue";

    public static final String YARN_RANGER_RESOURCE_TYPE = "queue";

    public static final String KAFKA_RESOURCE_TYPE = "topic";

    public static final String HDFS_NAMESPACE_QUOTA = "nameSpaceQuota";

    public static final String HDFS_NAMESPACE_QUOTA_PLAN = "Name Space Quota";

    public static final String HDFS_STORAGE_QUOTA = "storageSpaceQuota";

    public static final String HDFS_STORAGE_QUOTA_PLAN = "Storage Space Quota (GB)";

    public static final String HBASE_NAMESPACE_TABLE_QUOTA = "maximumTablesQuota";

    public static final String HBASE_NAMESPACE_TABLE_QUOTA_PLAN = "HBase Maximun Tables";

    public static final String HBASE_NAMESPACE_REGION_QUOTA = "maximumRegionsQuota";

    public static final String HBASE_NAMESPACE_REGION_QUOTA_PLAN = "HBase Maximun Regions";

    public static final String YARN_QUEUE_QUOTA = "yarnQueueQuota";

    public static final String YARN_QUEUE_QUOTA_PLAN = "Yarn Queue Quota (GB)";
}
