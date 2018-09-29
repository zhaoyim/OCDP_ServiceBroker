# Datafoundry service broker for OCDP Hadoop services
A service broker for OCDP Hadoop services by using the [Spring Cloud - Cloud Foundry Service Broker](https://github.com/spring-cloud/spring-cloud-cloudfoundry-service-broker).

# Overview

This project uses the Spring Cloud - Cloud Foundry Service Broker to implement OCDP Hadoop services.

## Getting Started

### 1 Configure connection properties
Configure properties (e.g. LDAP, kerberos, Hadoop ...) in configuration file myprops.properties and move it to /etc/configurations/myprops.properties:

	BROKER_USERNAME=admin
	BROKER_PASSWORD=admin
	BROKER_ID=ethanscluster
	ETCD_HOST=10.1.236.146
	ETCD_PORT=2379
	ETCD_USER=admin
	ETCD_PWD=admin
	LDAP_URL=ldap://10.1.236.146:389
	LDAP_USER_DN=cn=root,dc=asiainfo,dc=com
	LDAP_PASSWORD=ldap@#123
	LDAP_BASE=dc=asiainfo,dc=com
	LDAP_GROUP=root
	LDAP_GROUP_ID=600
	KRB_KDC_HOST=10.1.236.146
	KRB_USER_PRINCIPAL=admin/admin@ASIAINFO.COM
	KRB_KEYTAB_LOCATION=
	KRB_ADMIN_PASSWORD=123456
	KRB_REALM=ASIAINFO.COM
	KRB_KRB5FILEPATH=/etc/krb5.conf
	CLUSTER_NAME=ethanscluster
	RANGER_URL=http://10.1.236.19:6080
	RANGER_ADMIN_USER=admin
	RANGER_ADMIN_PASSWORD=admin
	HDFS_NAME_NODE=10.1.236.19
	HDFS_RPC_PORT=8020
	HDFS_SUPER_USER=nn/10.1.236.19@ASIAINFO.COM
	HDFS_USER_KEYTAB=/etc/secrets/nn.service.keytab
	HDFS_PORT=50070
	HDFS_NAMESERVICES=ethanscluster
	HDFS_NAMENODE1_ADDR=10.1.236.19:8020
	HDFS_NAMENODE2_ADDR=10.1.236.19:8020
	HDFS_NAMENODE1=nn1
	HDFS_NAMENODE2=nn2
	HBASE_MASTER_URL=http://10.1.236.19:16010
	HBASE_MASTER=10.1.236.19
	HBASE_REST_PORT=16010
	HBASE_MASTER_PRINCIPAL=hbase/10.1.236.19@ASIAINFO.COM
	HBASE_MASTER_USER_KEYTAB=/etc/secrets/hbase.service.keytab
	HBASE_ZOOKEEPER_QUORUM=10.1.236.19,10.1.236.20,10.1.236.21
	HBASE_ZOOKEEPER_CLIENT_PORT=2181
	HBASE_ZOOKEEPER_ZNODE_PARENT=/hbase-unsecure
	HIVE_HOST=10.1.236.19
	HIVE_PORT=10000
	HIVE_SUPER_USER=hive/10.1.236.19@ASIAINFO.COM
	HIVE_SUPER_USER_KEYTAB=/etc/secrets/hive.service.keytab
	AMBARI_HOST=http://10.1.236.19:8080
	AMBARI_ADMIN_USER=admin
	AMBARI_ADMIN_PWD=admin
	YARN_RESOURCEMANAGER_HOST=10.1.236.19
	YARN_RESOURCEMANAGER_PORT=8088
	YARN_RESOURCEMANAGER_URL=http://10.1.236.19:8088
	YARN_RESOURCEMANAGER_URL2=http://10.1.236.19:8088
	YARN_SUPER_USER=rm/10.1.236.19@ASIAINFO.COM
	YARN_SUPER_USER_KEYTAB=/etc/secrets/rm.service.keytab
	OC_ZK_CONNECTION=10.1.236.19:2181,10.1.236.20:2181,10.1.236.21:2181
	KAFKA_JAAS_PATH=/etc/secrets/kafka_jaas.conf
	KAFKA_HOSTS=10.1.236.19
	KAFKA_PORT=6667
	KAFKA_REP_FACTOR=2
	KRB_ENABLE=false
	MR_HISTORY_URL= 
	SPARK_THRIFT_SERVER= 
	SPARK_THRIFT_PORT= 
	SPARK_HISTORY_URL= 

### 2 Run OCDP service broker in VM:
Build OCDP service broker by gradle command:

    ./gradlew build

After building, you can run service broker by run "java -jar" command like below:

    java -jar build/libs/datafoundry-ocdp-service-broker.jar

### 3 Run OCDP service broker in docker container:
Overwrite krb5.conf and hdfs.keytab files in source code folder: src/main/docker/

    cp <path for krb5.conf> <path for hdfs.keytab> src/main/docker

Build OCDP service broker by gradle command:

    ./gradlew build buildDocker

Then you can start OCDP service broker container by docker command like below:

    docker run -p <local port>:8080 --add-host <hostname:ip> (host list for ldap/kdc/hadoop) -e <env_name='env_value'> (env list about connectivity properties) -t asiainfo-ldp/datafoundry-ocdp-service-broker

### 4 Test OCDP service broker
Service catalog:

    curl -H "X-Broker-API-Version: 2.8" http://<broker.username>:<broker.password>@localhost:8080/v2/catalog

HDFS service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-001?accepts_incomplete=true -d '{
      "service_id":"ae67d4ba-5c4e-4937-a68b-5b47cfe356d8",
      "plan_id":"72150b09-1025-4533-8bae-0e04ef68ac13",
      "organization_guid": "org-guid",
      "space_guid":"baikai",
      "parameters": {"ami_id":"ami-ecb68a84","nameSpaceQuota":"100000000000","storageSpaceQuota":"10000"}
    }' -H "Content-Type: application/json"

HDFS service instance update for assign role to tenant user

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-001?accepts_incomplete=true -d '{
       "service_id":"ae67d4ba-5c4e-4937-a68b-5b47cfe356d8",
       "plan_id":"72150b09-1025-4533-8bae-0e04ef68ac13",
       "parameters":{
            "user_name": "user1",
            "accesses": "read, write, execute"
       }
    }' -H "Content-Type: application/json"

HDFS service instance update for resize/scale

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-001 -d '{
               "service_id":"ae67d4ba-5c4e-4937-a68b-5b47cfe356d8",
               "plan_id":"72150b09-1025-4533-8bae-0e04ef68ac13",
               "parameters":{
                    "nameSpaceQuota":"100000000000",
                    "storageSpaceQuota":"10000"
               }
    }' -H "Content-Type: application/json"

HDFS service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-003/service_bindings/hdfs-binding-001 -d '{
      "plan_id":        "ae67d4ba-5c4e-4937-a68b-5b47cfe356d8"",
      "service_id":     "72150b09-1025-4533-8bae-0e04ef68ac13",
      "app_guid":       "app-guid",
      "parameters": {
          "user_name": "user1"
      }
    }' -H "Content-Type: application/json"

HDFS service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-002/service_bindings/hdfs-binding-001?service_id=ae67d4ba-5c4e-4937-a68b-5b47cfe356d8&plan_id=72150b09-1025-4533-8bae-0e04ef68ac13'

HDFS service deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-002?service_id=ae67d4ba-5c4e-4937-a68b-5b47cfe356d8&plan_id=72150b09-1025-4533-8bae-0e04ef68ac13'

HBase service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001?accepts_incomplete=true -d '{
      "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
      "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
      "organization_guid": "org-guid",
      "space_guid":"baikai",
      "parameters": {"ami_id":"ami-ecb68a84","maximumTablesQuota":"100000000000","maximumRegionsQuota":"10000000000000"}
    }' -H "Content-Type: application/json"

HBase service instance update for assign role to tenant user

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001?accepts_incomplete=true -d '{
       "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
       "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
       "parameters":{
            "user_name": "user1",
            "accesses": "read,write,create,admin"
       }
    }' -H "Content-Type: application/json"

HBase service instance update for resize/scale

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hdfs-shared-001 -d '{
               "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
               "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
               "parameters":{
                   "maximumTablesQuota":"100000000000",
                   "maximumRegionsQuota":"10000000000000"
               }
    }' -H "Content-Type: application/json"

HBase service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001/service_bindings/hbase-binding-001 -d '{
      "service_id":"d9845ade-9410-4c7f-8689-4e032c1a8450",
      "plan_id":"f658e391-b7d6-4b72-9e4c-c754e4943ae1",
      "app_guid":       "app-guid",
      "parameters": {
          "user_name": "user1"
      }
    }' -H "Content-Type: application/json"

HBase service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001/service_bindings/hbase-binding-001?service_id=d9845ade-9410-4c7f-8689-4e032c1a8450&plan_id=f658e391-b7d6-4b72-9e4c-c754e4943ae1'

HBase service instance deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hbase-shared-001?service_id=d9845ade-9410-4c7f-8689-4e032c1a8450&plan_id=f658e391-b7d6-4b72-9e4c-c754e4943ae1'

Hive service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001?accepts_incomplete=true -d '{
      "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
      "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
      "organization_guid": "org-guid",
      "space_guid":"baikai",
      "parameters": {"ami_id":"ami-ecb68a84","storageSpaceQuota":"1","yarnQueueQuota":"1"}
    }' -H "Content-Type: application/json"

Hive service instance update for assign role to tenant user

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001?accepts_incomplete=true -d '{
       "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
       "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
       "parameters":{
            "user_name": "user1",
            "accesses": "select,update,create,drop,alter,index,lock"
            }
       }
    }' -H "Content-Type: application/json"

Hive service instance update for resize/scale

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001 -d '{
               "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
               "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
               "parameters":{
                   "storageSpaceQuota":"2",
                   "yarnQueueQuota":"2"
               }
    }' -H "Content-Type: application/json"

Hive service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001/service_bindings/hive-binding-001 -d '{
      "service_id":"2ef26018-003d-4b2b-b786-0481d4ee9fa3",
      "plan_id":"aa7e364f-fdbf-4187-b60a-218b6fa398ed",
      "app_guid":       "app-guid",
      "parameters":{
          "user_name": "user1"
      }
    }' -H "Content-Type: application/json"

Hive service instance unbinding:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001/service_bindings/hive-binding-001?service_id=2ef26018-003d-4b2b-b786-0481d4ee9fa3&plan_id=aa7e364f-fdbf-4187-b60a-218b6fa398ed'

Hive service instance deprovision:

    curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/hive-shared-001?service_id=2ef26018-003d-4b2b-b786-0481d4ee9fa3&plan_id=aa7e364f-fdbf-4187-b60a-218b6fa398ed'

MapReduce service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001?accepts_incomplete=true -d '{
      "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
      "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
      "organization_guid": "org-guid",
      "space_guid":"baikai",
      "parameters": {"ami_id":"ami-ecb68a84","yarnQueueQuota": "1"}
    }' -H "Content-Type: application/json"

MapReduce service instance update for assign role to tenant user

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001?accepts_incomplete=true -d '{
       "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
       "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
       "parameters":{
            "user_name": "user1",
            "accesses": "submit-app,admin-queue"
            }
       }
    }' -H "Content-Type: application/json"

MapReduce service instance update for resize/scale

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001 -d '{
               "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
               "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
               "parameters":{
                   "yarnQueueQuota": "1"
               }
    }' -H "Content-Type: application/json"

MapReduce service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001/service_bindings/mr-binding-001 -d '{
      "service_id":"ae0f2324-27a8-415b-9c7f-64ab6cd88d40",
      "plan_id":"6524c793-0ea5-4456-9a60-ca70271decdc",
       "app_guid":       "app-guid",
       "parameters":{
           "user_name": "user1"
       }
    }' -H "Content-Type: application/json"

MapReduce service instance unbinding:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/mr-shared-001/service_bindings/mr-binding-001?service_id=ae0f2324-27a8-415b-9c7f-64ab6cd88d40&plan_id=6524c793-0ea5-4456-9a60-ca70271decdc'

MapReduce service instance deprovision:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080//v2/service_instances/mr-shared-001?service_id=ae0f2324-27a8-415b-9c7f-64ab6cd88d40&plan_id=6524c793-0ea5-4456-9a60-ca70271decdc'

Spark service instance provision:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001?accepts_incomplete=true -d '{
      "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
      "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
      "organization_guid": "org-guid",
      "space_guid":"baikai",
      "parameters": {"ami_id":"ami-ecb68a84","yarnQueueQuota": "1"}
    }' -H "Content-Type: application/json"

Spark service instance update for assign role to tenant user

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001?accepts_incomplete=true -d '{
       "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
       "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
       "parameters":{
            "user_name": "user1",
            "accesses": "submit-app,admin-queue"
            }
       }
    }' -H "Content-Type: application/json"

Spark service instance update for resize/scale

    curl -i -X PATCH http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001 -d '{
               "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
                "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
               "parameters":{
                   "yarnQueueQuota": "1"
               }
    }' -H "Content-Type: application/json"

Spark service instance binding:

    curl -i -X PUT http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001/service_bindings/spark-binding-001 -d '{
      "service_id":"d3b9a485-f038-4605-9b9b-29792f5c61d1",
      "plan_id":"5c3d471d-f94a-4bb8-b340-f783f3c15ba1",
       "app_guid": "app-guid",
       "parameters":{
           "user_name": "user1"
        }
    }' -H "Content-Type: application/json"

Spark service instance unbinding:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080/v2/service_instances/spark-shared-001/service_bindings/spark-binding-001?service_id=d3b9a485-f038-4605-9b9b-29792f5c61d1&plan_id=5c3d471d-f94a-4bb8-b340-f783f3c15ba1'

Spark service instance deprovision:

      curl -i -X DELETE 'http://<broker.username>:<broker.password>@localhost:8080//v2/service_instances/spark-shared-001?service_id=d3b9a485-f038-4605-9b9b-29792f5c61d1&plan_id=5c3d471d-f94a-4bb8-b340-f783f3c15ba1'