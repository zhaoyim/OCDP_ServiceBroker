package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import com.google.common.base.Strings;

@Configuration
@PropertySource(value = {"file:C:\\Users\\Ethan\\Desktop\\myprops.properties"})
public class ClusterConfig {	
	@Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
	
	@Value("${ETCD_HOST}")
	private String etcd_host;
	@Value("${ETCD_PORT}")
	private String etcd_port;
	@Value("${ETCD_USER}")
	private String etcd_user;
	@Value("${ETCD_PWD}")
	private String etcd_pwd;
	@Value("${LDAP_URL}")
	private String ldap_url;
	@Value("${LDAP_USER_DN}")
	private String ldap_userDN;
	@Value("${LDAP_PASSWORD}")
	private String ldap_password;
	@Value("${LDAP_BASE}")
	private String ldap_base;
	@Value("${LDAP_GROUP}")
	private String ldap_group;
	@Value("${LDAP_GROUP_ID}")
	private String ldap_group_id;
	@Value("${KRB_KDC_HOST}")
	private String krb_kdcHost;
	@Value("${KRB_USER_PRINCIPAL}")
	private String krb_userPrincipal;
	@Value("${KRB_KEYTAB_LOCATION}")
	private String krb_keytabLocation;
	@Value("${KRB_ADMIN_PASSWORD}")
	private String krb_adminPwd;
	@Value("${KRB_REALM}")
	private String krb_realm;
	@Value("${KRB_KRB5FILEPATH}")
	private String krb_krb5FilePath;
	@Value("${CLUSTER_NAME}")
	private String cluster_name;
	@Value("${RANGER_URL}")
	private String ranger_url;
	@Value("${RANGER_ADMIN_USER}")
	private String ranger_user;
	@Value("${RANGER_ADMIN_PASSWORD}")
	private String ranger_pwd;
	@Value("${HDFS_NAME_NODE}")
	private String hdfs_nameNode;
	@Value("${HDFS_RPC_PORT}")
	private String hdfs_rpcPort;
	@Value("${HDFS_PORT}")
	private String hdfs_port;
	@Value("${HDFS_SUPER_USER}")
	private String hdfs_superUser;
	@Value("${HDFS_USER_KEYTAB}")
	private String hdfs_userKeytab;
	@Value("${HDFS_NAMESERVICES}")
	private String hdfs_nameservices;
	@Value("${HDFS_NAMENODE1_ADDR}")
	private String hdfs_nameNode1;
	@Value("${HDFS_NAMENODE2_ADDR}")
	private String hdfs_nameNode2;
	@Value("${HDFS_NAMENODE1}")
	private String hdfs_nameNode1_addr;
	@Value("${HDFS_NAMENODE2}")
	private String hdfs_nameNode2_addr;
	@Value("${HBASE_MASTER_URL}")
	private String hbase_masterUrl;
	@Value("${HBASE_MASTER_PRINCIPAL}")
	private String hbase_masterPrincipal;
	@Value("${HBASE_MASTER_USER_KEYTAB}")
	private String hbase_masterUserKeytab;
	@Value("${HBASE_ZOOKEEPER_QUORUM}")
	private String hbase_zookeeper_quorum;
	@Value("${HBASE_ZOOKEEPER_CLIENT_PORT}")
	private String hbase_zookeeper_clientPort;
	@Value("${HBASE_ZOOKEEPER_ZNODE_PARENT}")
	private String hbase_zookeeper_znodeParent;
	@Value("${HBASE_MASTER}")
	private String hbase_master;
	@Value("${HBASE_REST_PORT}")
	private String hbase_restPort;
	@Value("${HIVE_HOST}")
	private String hive_host;
	@Value("${HIVE_PORT}")
	private String hive_port;
	@Value("${HIVE_SUPER_USER}")
	private String hive_superUser;
	@Value("$HIVE_SUPER_USER_KEYTAB{}")
	private String hive_superUserKeytab;
	@Value("${AMBARI_HOST}")
	private String ambari_host;
	@Value("${AMBARI_ADMIN_USER}")
	private String ambari_adminUser;
	@Value("${AMBARI_ADMIN_PWD}")
	private String ambari_adminPwd;
	@Value("${YARN_RESOURCEMANAGER_HOST}")
	private String yarn_rm_host;
	@Value("${YARN_RESOURCEMANAGER_PORT}")
	private String yarn_rm_port;
	@Value("${YARN_RESOURCEMANAGER_URL}")
	private String yarn_rm_url;
	@Value("${YARN_RESOURCEMANAGER_URL2}")
	private String yarn_rm_url2;
	@Value("${OC_ZK_CONNECTION}")
	private String zk_connection; // eg: ochadoop111.jcloud.local:2181
	@Value("${KAFKA_JAAS_PATH}")
	private String kafka_jaas_path;
	@Value("${KAFKA_HOSTS}")
	private String kafka_hosts;
	@Value("${KAFKA_PORT}")
	private String kafka_port;
	@Value("${KAFKA_REP_FACTOR}")
	private String kafka_rep;
	@Value("${KRB_ENABLE}")
	private boolean krb_enable = true;
	@Value("${BROKER_ID}")
	private String brokerId;
	@Value("${BROKER_USERNAME}")
	private String brokerUser;
	@Value("${BROKER_PASSWORD}")
	private String brokerPassword;
	@Value("${MR_HISTORY_URL}")
	private String mr_history_url;
	@Value("${SPARK_THRIFT_SERVER}")
	private String spark_thrift_server;
	@Value("${SPARK_THRIFT_PORT}")
	private String spark_thrift_port;
	@Value("${SPARK_HISTORY_URL}")
	private String spark_history_url;
	@Value("${KMS_IP}")
	private String kms_ip;
	@Value("${KMS_PORT}")
	private String kms_port;
	@Value("${KMS_ADMIN_NAME}")
	private String kms_admin_name;
	@Value("${KMS_ADMIN_PASSWORD}")
	private String kms_admin_password;
	@Value("${HADOOP_USER_NAME}")
	private String hadoop_user_name;
	
	public String getHadoop_user_name() {
		return hadoop_user_name;
	}

	public String getKms_ip() {
		return kms_ip;
	}

	public String getKms_port() {
		return kms_port;
	}

	public String getKms_admin_name() {
		return kms_admin_name;
	}

	public String getKms_admin_password() {
		return kms_admin_password;
	}

	public boolean krbEnabled() {
		return krb_enable;
	}

	public String getBrokerUser() {
		return brokerUser;
	}

	public String getBrokerPassword() {
		return brokerPassword;
	}

	public String getEtcdHost() {
		return etcd_host;
	}

	public String getEtcdPort() {
		return etcd_port;
	}

	public String getEtcdUser() {
		return etcd_user;
	}

	public String getEtcd_pwd() {
		return etcd_pwd;
	}

	public String getLdapUrl() {
		return ldap_url;
	}

	public String getLdapUserDN() {
		return ldap_userDN;
	}

	public String getLdapPassword() {
		return ldap_password;
	}

	public String getLdapBase() {
		return ldap_base;
	}

	public String getLdapGroup() {
		return ldap_group;
	}

	public String getLdapGroupId() {
		return ldap_group_id;
	}

	public String getKrbKdcHost() {
		return krb_kdcHost;
	}

	public String getKrbUserPrincipal() {
		return krb_userPrincipal;
	}

	public String getKrbKeytabLocation() {
		return krb_keytabLocation;
	}

	public String getKrbAdminPwd() {
		return krb_adminPwd;
	}

	public String getKrbRealm() {
		return krb_realm;
	}

	public String getKrb5FilePath() {
		return krb_krb5FilePath;
	}

	public String getClusterName() {
		return cluster_name;
	}

	public String getRangerUrl() {
		return ranger_url;
	}

	public String getRangerUser() {
		return ranger_user;
	}

	public String getRangerPwd() {
		return ranger_pwd;
	}

	public String getHdfsNameNode() {
		return hdfs_nameNode;
	}

	public String getHdfsRpcPort() {
		return hdfs_rpcPort;
	}

	public String getHdfsPort() {
		return hdfs_port;
	}

	public String getHdfsSuperUser() {
		return hdfs_superUser;
	}

	public String getHdfsUserKeytab() {
		return hdfs_userKeytab;
	}

	public String getHdfsNameservices() {
		return hdfs_nameservices;
	}

	public String getHdfs_nameNode1() {
		return hdfs_nameNode1;
	}

	public String getHdfs_nameNode2() {
		return hdfs_nameNode2;
	}

	public String getHdfs_nameNode1_addr() {
		return hdfs_nameNode1_addr;
	}

	public String getHdfs_nameNode2_addr() {
		return hdfs_nameNode2_addr;
	}

	public String getHbaseMasterUrl() {
		return hbase_masterUrl;
	}

	public String getHbaseMasterPrincipal() {
		return hbase_masterPrincipal;
	}

	public String getHbaseMasterUserKeytab() {
		return hbase_masterUserKeytab;
	}

	public String getHbaseZookeeperQuorum() {
		return hbase_zookeeper_quorum;
	}

	public String getHbaseZookeeperClientPort() {
		return hbase_zookeeper_clientPort;
	}

	public String getHbaseZookeeperZnodeParent() {
		return hbase_zookeeper_znodeParent;
	}

	public String getHbaseMaster() {
		return hbase_master;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public String getHbaseRestPort() {
		return hbase_restPort;
	}

	public String getHiveHost() {
		return hive_host;
	}

	public String getHivePort() {
		return hive_port;
	}

	public String getHiveSuperUser() {
		return hive_superUser;
	}

	public String getHiveSuperUserKeytab() {
		return hive_superUserKeytab;
	}

	public String getAmbari_host() {
		return ambari_host;
	}

	public String getAmbari_adminUser() {
		return ambari_adminUser;
	}

	public String getAmbari_adminPwd() {
		return ambari_adminPwd;
	}

	public String getYarnRMHost() {
		return yarn_rm_host;
	}

	public String getYarnRMPort() {
		return yarn_rm_port;
	}

	public String getYarnRMUrl() {
		return yarn_rm_url;
	}

	public etcdClient getEtcdClient() {
		if (Strings.isNullOrEmpty(brokerId)) {
			System.out.println("ERROR: BROKER_ID is null in configurations.");
			throw new RuntimeException("BROKER_ID can not be null in configurations.");
		}
		return new etcdClient(etcd_host, etcd_port, etcd_user, etcd_pwd, brokerId);
	}

	@Bean
	public LdapContextSource getLdapContextSource() {
		LdapContextSource contextSource = new LdapContextSource();
		contextSource.setUrl(ldap_url);
		contextSource.setUserDn(ldap_userDN);
		contextSource.setPassword(ldap_password);
		contextSource.setBase(ldap_base);
		return contextSource;
	}

	@Bean
	public LdapTemplate getLdapTemplate() {
		return new LdapTemplate(this.getLdapContextSource());
	}

	@Bean
	public rangerClient getRangerClient() {
		return new rangerClient(ranger_url, ranger_user, ranger_pwd);
	}

	@Bean
	public ambariClient getAmbariClient() {
		return new ambariClient(ambari_host, ambari_adminUser, ambari_adminPwd, cluster_name);
	}

	@Bean
	public yarnClient getYarnClient() {
		if (yarn_rm_url2 != null)
			return new yarnClient(yarn_rm_url, yarn_rm_url2);
		else
			return new yarnClient(yarn_rm_url);
	}

	public String getZk_connection() {
		return zk_connection;
	}

	public String getKafka_jaas_path() {
		return kafka_jaas_path;
	}

	public String getKafka_hosts() {
		return kafka_hosts;
	}

	public String getKafka_port() {
		return kafka_port;
	}

	public String getKafka_rep() {
		return kafka_rep;
	}

	public String getSparkThriftServer() {
		return spark_thrift_server;
	}

	public String getSparkThriftPort() {
		return spark_thrift_port;
	}

	@Override
	public String toString() {
		return "ClusterConfig [etcd_host=" + etcd_host + ", etcd_port=" + etcd_port + ", etcd_user=" + etcd_user
				+ ", etcd_pwd=" + etcd_pwd + ", ldap_url=" + ldap_url + ", ldap_userDN=" + ldap_userDN
				+ ", ldap_password=" + ldap_password + ", ldap_base=" + ldap_base + ", ldap_group=" + ldap_group
				+ ", ldap_group_id=" + ldap_group_id + ", krb_kdcHost=" + krb_kdcHost + ", krb_userPrincipal="
				+ krb_userPrincipal + ", krb_keytabLocation=" + krb_keytabLocation + ", krb_adminPwd=" + krb_adminPwd
				+ ", krb_realm=" + krb_realm + ", krb_krb5FilePath=" + krb_krb5FilePath + ", cluster_name="
				+ cluster_name + ", ranger_url=" + ranger_url + ", ranger_user=" + ranger_user + ", ranger_pwd="
				+ ranger_pwd + ", hdfs_nameNode=" + hdfs_nameNode + ", hdfs_rpcPort=" + hdfs_rpcPort + ", hdfs_port="
				+ hdfs_port + ", hdfs_superUser=" + hdfs_superUser + ", hdfs_userKeytab=" + hdfs_userKeytab
				+ ", hdfs_nameservices=" + hdfs_nameservices + ", hdfs_nameNode1=" + hdfs_nameNode1
				+ ", hdfs_nameNode2=" + hdfs_nameNode2 + ", hdfs_nameNode1_addr=" + hdfs_nameNode1_addr
				+ ", hdfs_nameNode2_addr=" + hdfs_nameNode2_addr + ", hbase_masterUrl=" + hbase_masterUrl
				+ ", hbase_masterPrincipal=" + hbase_masterPrincipal + ", hbase_masterUserKeytab="
				+ hbase_masterUserKeytab + ", hbase_zookeeper_quorum=" + hbase_zookeeper_quorum
				+ ", hbase_zookeeper_clientPort=" + hbase_zookeeper_clientPort + ", hbase_zookeeper_znodeParent="
				+ hbase_zookeeper_znodeParent + ", hbase_master=" + hbase_master + ", hbase_restPort=" + hbase_restPort
				+ ", hive_host=" + hive_host + ", hive_port=" + hive_port + ", hive_superUser=" + hive_superUser
				+ ", hive_superUserKeytab=" + hive_superUserKeytab + ", ambari_host=" + ambari_host
				+ ", ambari_adminUser=" + ambari_adminUser + ", ambari_adminPwd=" + ambari_adminPwd + ", yarn_rm_host="
				+ yarn_rm_host + ", yarn_rm_port=" + yarn_rm_port + ", yarn_rm_url=" + yarn_rm_url + ", yarn_rm_url2="
				+ yarn_rm_url2 + ", zk_connection=" + zk_connection + ", kafka_jaas_path=" + kafka_jaas_path
				+ ", kafka_hosts=" + kafka_hosts + ", kafka_port=" + kafka_port + ", kafka_rep=" + kafka_rep
				+ ", krb_enable=" + krb_enable + ", brokerId=" + brokerId + ", brokerUser=" + brokerUser
				+ ", brokerPassword=" + brokerPassword + ", mr_history_url=" + mr_history_url + ", spark_thrift_server="
				+ spark_thrift_server + ", spark_thrift_port=" + spark_thrift_port + ", spark_history_url="
				+ spark_history_url + ", kms_ip=" + kms_ip + ", kms_port=" + kms_port + ", kms_admin_name="
				+ kms_admin_name + ", kms_admin_password=" + kms_admin_password + ", hadoop_user_name="
				+ hadoop_user_name + "]";
	}

}
