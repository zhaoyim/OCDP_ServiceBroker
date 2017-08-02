package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HBaseAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HBaseAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private Configuration conf;

    private Connection connection;

    private static final List<String> ACCESSES = Lists.newArrayList("read", "write", "create", "admin");

    @Autowired
    public HBaseAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        System.setProperty("java.security.krb5.conf", this.clusterConfig.getKrb5FilePath());
        this.conf = HBaseConfiguration.create();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.master.kerberos.principal", this.clusterConfig.getHbaseMasterPrincipal());
        conf.set("hbase.master.keytab.file", this.clusterConfig.getHbaseMasterUserKeytab());
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.clusterConfig.getHbaseZookeeperQuorum());
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.clusterConfig.getHbaseZookeeperClientPort());
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.clusterConfig.getHbaseZookeeperZnodeParent());
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> cuzQuota) throws Exception{
        String nsName = serviceInstanceId.replaceAll("-", "");
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHbaseMasterPrincipal(), this.clusterConfig.getHbaseMasterUserKeytab());
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = this.connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxtables", quota.get(OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA));
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxregions", quota.get(OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA));
            admin.createNamespace(namespaceDescriptor);
            admin.close();
        }catch(IOException e){
            logger.error("HBase namespace create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            this.connection.close();
        }
        return nsName;
    }

    @Override
    public String createPolicyForResources(String policyName, List<String> tableList, List<String> userList,
                                           String groupName, List<String> permissions){
        logger.info("Assign read/write/create/admin permission to hbase namespace.");
        String policyId = null;
        String serviceName = clusterConfig.getClusterName()+"_hbase";
        ArrayList<String> cfList = Lists.newArrayList("*");
        ArrayList<String> cList = Lists.newArrayList("*");
        ArrayList<String> groupList = Lists.newArrayList(groupName);
       // ArrayList<String> userList = new ArrayList<String>(){{add(userName);}};
       // ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("create");add("admin");}};
        ArrayList<String> conditions = Lists.newArrayList();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is HBase Policy",serviceName,true,true);
        ArrayList<String> nsList = new ArrayList<String>();
        // Convert namespace name to 'ns:*'
        for (String e : tableList){
            nsList.add(e + ":*");
        }
        rp.addResources(OCDPConstants.HBASE_RANGER_RESOURCE_TYPE, nsList, false);
        rp.addResources("column-family", cfList, false);
        rp.addResources("column", cList, false);
        if (permissions == null) {
            rp.addPolicyItems(userList,groupList,conditions,false,ACCESSES);
        } else {
            rp.addPolicyItems(userList,groupList,conditions,false,permissions);
        }
        String newPolicyString = rc.createV2Policy(serviceName, rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        return rc.appendResourceToV2Policy(policyId, serviceInstanceResource, OCDPConstants.HBASE_RANGER_RESOURCE_TYPE);
    }

    @Override
    public boolean appendUsersToPolicy(
            String policyId, String groupName, List<String> users, List<String> permissions){
        return rc.appendUsersToV2Policy(policyId, groupName, users, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHbaseMasterPrincipal(),
                    this.clusterConfig.getHbaseMasterUserKeytab());
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = this.connection.getAdmin();
            // Should drop all tables under such namespace
            TableName[] tableNames = admin.listTableNamesByNamespace(serviceInstanceResuorceName);
            for (TableName name : tableNames) {
                admin.disableTable(name);
                admin.deleteTable(name);
            }
            admin.deleteNamespace(serviceInstanceResuorceName);
            admin.close();
            logger.info("Delete HBase namespace successful.");
        }catch (IOException e){
            logger.error("HBase namespace delete fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.connection.close();
        }
    }

    @Override
    public boolean deletePolicyForResources(String policyId){
        logger.info("Unassign read/write/create/admin permission to hbase namespace.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource){
        return rc.removeResourceFromV2Policy(
                policyId, serviceInstanceResource, OCDPConstants.HBASE_RANGER_RESOURCE_TYPE);
    }

    @Override
    public boolean removeUserFromPolicy(String policyId, String userName){
        return rc.removeUserFromV2Policy(policyId, userName);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        return new HashMap<String, Object>(){
            {
                put("uri", "http://" + clusterConfig.getHbaseMaster() + ":" + clusterConfig.getHbaseRestPort());
                put("host", clusterConfig.getHbaseMaster());
                put("port", clusterConfig.getHbaseRestPort());
                put(OCDPConstants.HBASE_RESOURCE_TYPE, serviceInstanceId.replaceAll("-", ""));
            }
        };
    }

    @Override
    public  List<String> getResourceFromPolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, OCDPConstants.HBASE_RANGER_RESOURCE_TYPE);
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String planId = instance.getPlanId();
        Map<String, String> quota = getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String ns = (String)instance.getServiceInstanceCredentials().get(resourceType);
        try{
            BrokerUtil.authentication(conf, clusterConfig.getHbaseMasterPrincipal(),
                    clusterConfig.getHbaseMasterUserKeytab());
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(ns);
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxtables", quota.get(OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA));
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxregions", quota.get(OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA));
            admin.modifyNamespace(namespaceDescriptor);
            admin.close();
        } catch (IOException e){
            e.printStackTrace();
            throw e;
        } finally {
            connection.close();
        }
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
    }

}
