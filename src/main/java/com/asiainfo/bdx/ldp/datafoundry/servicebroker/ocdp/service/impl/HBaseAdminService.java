package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
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
                                     String bindingId, Map<String, Object> cuzQuota) throws Exception{
        String nsName = serviceInstanceId.replaceAll("-", "");
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHbaseMasterPrincipal(), this.clusterConfig.getHbaseMasterUserKeytab());
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = this.connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();
            namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxtables", quota.get("maximumTableQuota"));
            namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxregion", quota.get("maximunRegionQuota"));
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
    public String createPolicyForTenant(String policyName, List<String> tableList, String tenantName, String groupName){
        logger.info("Assign read/write/create/admin permission to hbase namespace.");
        String policyId = null;
        ArrayList<String> cfList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> cList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(tenantName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("create");add("admin");}};
        ArrayList<String> conditions = new ArrayList<String>();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is HBase Policy", clusterConfig.getClusterName()+"_hbase",true,true);
        ArrayList<String> nsList = new ArrayList<String>();
        // Convert namespace name to 'ns:*'
        for (String e : tableList){
            nsList.add(e + ":*");
        }
        rp.addResources("table", nsList, false);
        rp.addResources("column-family", cfList, false);
        rp.addResources("column", cList, false);
        rp.addPolicyItems(userList,groupList,conditions,false,types);
        String newPolicyString = rc.createV2Policy(rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        return policyId;
    }

    @Override
    public boolean appendResourceToTenantPolicy(String policyId, String serviceInstanceResource){
        return rc.appendResourceToV2Policy(policyId, serviceInstanceResource, "table");
    }

    @Override
    public boolean appendUserToTenantPolicy(String policyId, String groupName, String accountName, List<String> permissions){
        return rc.appendUserToV2Policy(policyId, groupName, accountName, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHbaseMasterPrincipal(), this.clusterConfig.getHbaseMasterUserKeytab());
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
    public boolean deletePolicyForTenant(String policyId){
        logger.info("Unassign read/write/create/admin permission to hbase namespace.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource){
        return rc.removeResourceFromV2Policy(policyId, serviceInstanceResource, "table");
    }

    @Override
    public boolean removeUserFromTenantPolicy(String policyId, String accountName){
        return rc.removeUserFromV2Policy(policyId, accountName);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        return new HashMap<String, Object>(){
            {
                put("uri", "http://" + clusterConfig.getHbaseMaster() + ":" + clusterConfig.getHbaseRestPort());
                put("host", clusterConfig.getHbaseMaster());
                put("port", clusterConfig.getHbaseRestPort());
                put("HBase NameSpace", serviceInstanceId.replaceAll("-", ""));
            }
        };
    }

    @Override
    public  List<String> getResourceFromTenantPolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, "table");
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) {

    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        Object customize = metadata.get("customize");
        String maximumTableQuota, maximumRegionQuota;
        if(customize != null){
            // Customize quota case
            Map<String, Object> customizeMap = (Map<String,Object>)customize;

            CustomizeQuotaItem maximumTableQuotaItem = (CustomizeQuotaItem)customizeMap.get("maximumTablesQuota");
            long defaultMaximumTableQuota= maximumTableQuotaItem.getDefault();
            long maxMaximumTableQuota = maximumTableQuotaItem.getMax();

            CustomizeQuotaItem maximumRegionQuotaItem = (CustomizeQuotaItem)customizeMap.get("maximumRegionsQuota");
            long defaultMaximumRegionQuota = maximumRegionQuotaItem.getDefault();
            long maxMaximumRegionQuota = maximumRegionQuotaItem.getMax();

            if (cuzQuota.get("maximumTablesQuota") != null && cuzQuota.get("maximumRegionsQuota") != null){
                // customize quota have input value
                maximumTableQuota = (String)cuzQuota.get("maximumTablesQuota");
                maximumRegionQuota = (String)cuzQuota.get("maximumRegionsQuota");
                // If customize quota exceeds plan limitation, use default value
                if (Long.parseLong(maximumTableQuota) > maxMaximumTableQuota){
                    maximumTableQuota = Long.toString(defaultMaximumTableQuota);
                }
                if(Long.parseLong(maximumRegionQuota) > maxMaximumRegionQuota){
                    maximumRegionQuota = Long.toString(defaultMaximumRegionQuota);
                }

            }else {
                // customize quota have not input value, use default value
                maximumTableQuota =  Long.toString(defaultMaximumTableQuota);
                maximumRegionQuota =  Long.toString(defaultMaximumRegionQuota);
            }
        }else{
            // Non customize quota case, use plan.metadata.bullets
            List<String> bullets = (ArrayList)metadata.get("bullets");
            maximumTableQuota = bullets.get(0).split(":")[1];
            maximumRegionQuota = bullets.get(1).split(":")[1];
        }
        Map<String, String> quota = new HashMap<>();
        quota.put("maximumTablesQuota", maximumTableQuota);
        quota.put("maximumRegionsQuota", maximumRegionQuota);
        return quota;
    }
}
