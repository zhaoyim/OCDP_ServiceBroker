package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.context.EnvironmentAware;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;


/**
 * Created by baikai on 5/28/16.
 */
@Configuration
public class ClusterConfig implements EnvironmentAware{

    // Etcd connectivity properties
    private String etcd_host;

    private String etcd_port;

    private String etcd_user;

    private String etcd_pwd;

    // LDAP connectivity properties
    private String ldap_url;

    private String ldap_userDN;

    private String ldap_password;

    private String ldap_base;

    private String ldap_group;

    private String ldap_group_id;

    // Kerberos connectivity properties
    private String krb_kdcHost;

    private String krb_userPrincipal;

    private String krb_keytabLocation;

    private String krb_adminPwd;

    private String krb_realm;

    private String krb_krb5FilePath;

    // Hadoop cluster name
    private String cluster_name;

    // Hadoop Ranger connectivity properties
    private String ranger_url;

    private String ranger_user;

    private String ranger_pwd;

    // Hadoop HDFS connectivity properties
    private String hdfs_nameNode;

    private String hdfs_rpcPort;

    private String hdfs_port;

    private String hdfs_superUser;

    private String hdfs_userKeytab;

    private String hdfs_nameservices;

    private String hdfs_nameNode1;

    private String hdfs_nameNode2;

    private String hdfs_nameNode1_addr;

    private String hdfs_nameNode2_addr;

    // Hadoop HBase connectivity properties
    private String hbase_masterUrl;

    private String hbase_masterPrincipal;

    private String hbase_masterUserKeytab;

    private String hbase_zookeeper_quorum;

    private String hbase_zookeeper_clientPort;

    private String hbase_zookeeper_znodeParent;

    private String hbase_master;

    private String hbase_restPort;

    // Hadoop Hive connectivity properties
    private String hive_host;

    private String hive_port;

    private String hive_superUser;

    private String hive_superUserKeytab;

    //Hadoop Ambari connectivity properties
    private String ambari_host;

    private String ambari_adminUser;

    private String ambari_adminPwd;

    //Hadoop Yarn Resource Manager properties
    private String yarn_rm_host;

    private String yarn_rm_port;

    private String yarn_rm_url;
    private String yarn_rm_url2;

    private String yarn_superUser;

    private String yarn_superUserKeytab;

    //Hadoop MapReduce History server
    private String mr_history_url;

    //Hadoop Spark History server
    private String spark_history_url;

    @Override
    public void setEnvironment(Environment env){
        this.etcd_host = env.getProperty("ETCD_HOST");
        this.etcd_port = env.getProperty("ETCD_PORT");
        this.etcd_user = env.getProperty("ETCD_USER");
        this.etcd_pwd = env.getProperty("ETCD_PWD");
        this.ldap_url = env.getProperty("LDAP_URL");
        this.ldap_userDN = env.getProperty("LDAP_USER_DN");
        this.ldap_password = env.getProperty("LDAP_PASSWORD");
        this.ldap_base = env.getProperty("LDAP_BASE");
        this.ldap_group = env.getProperty("LDAP_GROUP");
        this.ldap_group_id = env.getProperty("LDAP_GROUP_ID");
        this.krb_kdcHost = env.getProperty("KRB_KDC_HOST");
        this.krb_userPrincipal = env.getProperty("KRB_USER_PRINCIPAL");
        this.krb_keytabLocation = env.getProperty("KRB_KEYTAB_LOCATION");
        this.krb_adminPwd = env.getProperty("KRB_ADMIN_PASSWORD");
        this.krb_realm = env.getProperty("KRB_REALM");
        this.krb_krb5FilePath = env.getProperty("KRB_KRB5FILEPATH");
        this.cluster_name = env.getProperty("CLUSTER_NAME");
        this.ranger_url = env.getProperty("RANGER_URL");
        this.ranger_user = env.getProperty("RANGER_ADMIN_USER");
        this.ranger_pwd = env.getProperty("RANGER_ADMIN_PASSWORD");
        this.hdfs_nameNode = env.getProperty("HDFS_NAME_NODE");
        this.hdfs_rpcPort = env.getProperty("HDFS_RPC_PORT");
        this.hdfs_port = env.getProperty("HDFS_PORT");
        this.hdfs_superUser = env.getProperty("HDFS_SUPER_USER");
        this.hdfs_userKeytab = env.getProperty("HDFS_USER_KEYTAB");
        this.hdfs_nameservices = env.getProperty("HDFS_NAMESERVICES");
        this.hdfs_nameNode1 = env.getProperty("HDFS_NAMENODE1");
        this.hdfs_nameNode2 = env.getProperty("HDFS_NAMENODE2");
        this.hdfs_nameNode1_addr = env.getProperty("HDFS_NAMENODE1_ADDR");
        this.hdfs_nameNode2_addr = env.getProperty("HDFS_NAMENODE2_ADDR");


        this.hbase_masterUrl = env.getProperty("HBASE_MASTER_URL");
        this.hbase_masterPrincipal = env.getProperty("HBASE_MASTER_PRINCIPAL");
        this.hbase_masterUserKeytab = env.getProperty("HBASE_MASTER_USER_KEYTAB");
        this.hbase_zookeeper_quorum = env.getProperty("HBASE_ZOOKEEPER_QUORUM");
        this.hbase_zookeeper_clientPort = env.getProperty("HBASE_ZOOKEEPER_CLIENT_PORT");
        this.hbase_zookeeper_znodeParent = env.getProperty("HBASE_ZOOKEEPER_ZNODE_PARENT");
        this.hbase_master = env.getProperty("HBASE_MASTER");
        this.hbase_restPort = env.getProperty("HBASE_REST_PORT");
        this.hive_host = env.getProperty("HIVE_HOST");
        this.hive_port = env.getProperty("HIVE_PORT");
        this.hive_superUser = env.getProperty("HIVE_SUPER_USER");
        this.hive_superUserKeytab = env.getProperty("HIVE_SUPER_USER_KEYTAB");
        this.ambari_host = env.getProperty("AMBARI_HOST");
        this.ambari_adminUser = env.getProperty("AMBARI_ADMIN_USER");
        this.ambari_adminPwd = env.getProperty("AMBARI_ADMIN_PWD");
        this.yarn_rm_host = env.getProperty("YARN_RESOURCEMANAGER_HOST");
        this.yarn_rm_port = env.getProperty("YARN_RESOURCEMANAGER_PORT");
        this.yarn_rm_url = env.getProperty("YARN_RESOURCEMANAGER_URL");
        this.yarn_rm_url2 = env.getProperty("YARN_RESOURCEMANAGER_URL2");
        this.yarn_superUser = env.getProperty("YARN_SUPER_USER");
        this.yarn_superUserKeytab = env.getProperty("YARN_SUPER_USER_KEYTAB");
        this.mr_history_url = env.getProperty("MR_HISTORY_URL");
        this.spark_history_url = env.getProperty("SPARK_HISTORY_URL");
    }

    public String getEtcdHost() { return etcd_host; }
    public String getEtcdPort() { return etcd_port; }
    public String getEtcdUser() { return etcd_user; }
    public String getEtcd_pwd() { return etcd_pwd; }

    public String getLdapUrl() { return ldap_url; }
    public String getLdapUserDN() { return ldap_userDN; }
    public String getLdapPassword() { return ldap_password; }
    public String getLdapBase() { return ldap_base; }
    public String getLdapGroup() { return ldap_group; }
    public String getLdapGroupId() { return ldap_group_id; }

    public String getKrbKdcHost() { return krb_kdcHost; }
    public String getKrbUserPrincipal() { return krb_userPrincipal; }
    public String getKrbKeytabLocation() { return krb_keytabLocation; }
    public String getKrbAdminPwd() { return krb_adminPwd; }
    public String getKrbRealm() { return krb_realm; }
    public String getKrb5FilePath() { return krb_krb5FilePath; }

    public String getClusterName() { return cluster_name; }
    public String getRangerUrl() { return ranger_url; }
    public String getRangerUser() { return ranger_user; }
    public String getRangerPwd() { return ranger_pwd; }

    public String getHdfsNameNode() { return hdfs_nameNode;}
    public String getHdfsRpcPort() { return hdfs_rpcPort; }
    public String getHdfsPort() { return hdfs_port; }
    public String getHdfsSuperUser() { return hdfs_superUser; }
    public String getHdfsUserKeytab() { return hdfs_userKeytab; }
    public String getHdfsNameservices() { return hdfs_nameservices; }

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

    public String getHbaseMasterUrl() { return hbase_masterUrl; }
    public String getHbaseMasterPrincipal() { return hbase_masterPrincipal; }
    public String getHbaseMasterUserKeytab() { return hbase_masterUserKeytab; }
    public String getHbaseZookeeperQuorum() { return hbase_zookeeper_quorum; }
    public String getHbaseZookeeperClientPort() { return hbase_zookeeper_clientPort; }
    public String getHbaseZookeeperZnodeParent() { return hbase_zookeeper_znodeParent; }
    public String getHbaseMaster() { return hbase_master; }
    public String getHbaseRestPort() { return hbase_restPort; }

    public String getHiveHost() { return hive_host; }
    public String getHivePort() { return hive_port; }
    public String getHiveSuperUser() { return hive_superUser; }
    public String getHiveSuperUserKeytab() { return hive_superUserKeytab; }

    public String getAmbari_host(){return ambari_host;}
    public String getAmbari_adminUser(){return ambari_adminUser;}
    public String getAmbari_adminPwd(){return ambari_adminPwd;}

    public String getYarnRMHost(){return yarn_rm_host;}
    public String getYarnRMPort() { return yarn_rm_port; }
    public String getYarnRMUrl(){return yarn_rm_url;}
    public String getYarnSuperUser(){return yarn_superUser;}
    public String getYarnSuperUserKeytab(){return yarn_superUserKeytab;}

    public String getMRHistoryURL() { return mr_history_url; }

    public String getSparkHistoryURL() { return spark_history_url; }

    public etcdClient getEtcdClient(){
        return new etcdClient(etcd_host, etcd_port, etcd_user, etcd_pwd);
    }

    @Bean
    public LdapContextSource getLdapContextSource(){
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(ldap_url);
        contextSource.setUserDn(ldap_userDN);
        contextSource.setPassword(ldap_password);
        contextSource.setBase(ldap_base);
        return contextSource;
    }

    @Bean
    public LdapTemplate getLdapTemplate(){
        return new LdapTemplate(this.getLdapContextSource());
    }

    @Bean
    public rangerClient getRangerClient(){
        return new rangerClient(ranger_url, ranger_user, ranger_pwd);
    }

    @Bean
    public ambariClient getAmbariClient(){
        return new ambariClient(ambari_host,ambari_adminUser,ambari_adminPwd, cluster_name);
    }

    @Bean
    public yarnClient getYarnClient(){
        if (yarn_rm_url2 != null)
            return new yarnClient(yarn_rm_url,yarn_rm_url2);
        else
            return new yarnClient(yarn_rm_url);
    }

}
