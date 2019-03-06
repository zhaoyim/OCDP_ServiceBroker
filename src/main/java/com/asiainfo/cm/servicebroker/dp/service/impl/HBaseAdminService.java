package com.asiainfo.cm.servicebroker.dp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.asiainfo.cm.servicebroker.dp.client.rangerClient;
import com.asiainfo.cm.servicebroker.dp.config.CatalogConfig;
import com.asiainfo.cm.servicebroker.dp.config.ClusterConfig;
import com.asiainfo.cm.servicebroker.dp.model.RangerV2Policy;
import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;
import com.asiainfo.cm.servicebroker.dp.service.OCDPAdminService;
import com.asiainfo.cm.servicebroker.dp.utils.BrokerUtil;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPAdminServiceMapper;
import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;
import com.google.common.collect.Lists;
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
    
    private boolean krb_enabled;
    
    private static final Pattern REG = Pattern.compile("/(\\w|.)*@");

    @Autowired
    public HBaseAdminService(ClusterConfig clusterConfig){
    	logger.info("ClusterConfig: " + clusterConfig);
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();
        
        this.krb_enabled = this.clusterConfig.krbEnabled();
        logger.info("Kerberos enabled: " + this.krb_enabled);
        
        this.conf = HBaseConfiguration.create();

        if (krb_enabled) {
            System.setProperty("java.security.krb5.conf", this.clusterConfig.getKrb5FilePath());
            conf.set("hadoop.security.authentication", "Kerberos");
            conf.set("hbase.security.authentication", "Kerberos");
            conf.set("hbase.master.kerberos.principal", toCommonString(this.clusterConfig.getHbaseMasterPrincipal()));
//            conf.set("hbase.master.keytab.file", this.clusterConfig.getHbaseMasterUserKeytab());
		}
        
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.clusterConfig.getHbaseZookeeperQuorum());
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.clusterConfig.getHbaseZookeeperClientPort());
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.clusterConfig.getHbaseZookeeperZnodeParent());
    }

    private String toCommonString(String hbaseMasterPrincipal) {
    	String principal = REG.matcher(hbaseMasterPrincipal).replaceFirst("/_HOST@");
    	logger.debug("HMaster principal [{}] transformed to [{}]", hbaseMasterPrincipal, principal);
    	return principal;
	}

	private QuotaSettings getQpsQuotaSettings(String namespaceName, String type, String value) {

		QuotaSettings settings = null;
		String[] valueArray = value.split("/");
		try {
			// if the parameters is the correct 2 value
			if (valueArray.length == 2) {
				settings = QuotaSettingsFactory.throttleNamespace(namespaceName, ThrottleType.valueOf(type),
						Long.parseLong(valueArray[0]), TimeUnit.valueOf(valueArray[1].toUpperCase()));
			}
		} catch (NumberFormatException e) {
			logger.error("HBase set namespace qps quota " + type + " fail due to: " + e.getLocalizedMessage());
			//e.printStackTrace();
		} catch (IllegalArgumentException e) {
			logger.error("HBase set namespace qps quota " + type + " fail due to: " + e.getLocalizedMessage());
			//e.printStackTrace();
		}
		return settings;
	}

	@Override
	public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
			Map<String, Object> parameters) throws Exception {
		String nsName = parameters.get("cuzBsiName") == null ? serviceInstanceId.replaceAll("-", "")
				: String.valueOf(parameters.get("cuzBsiName")).replaceAll("-", "");
		// support the Hbase qps quota
		String qpsRequestSize = parameters.get("REQUEST_SIZE") == null ? null
				: String.valueOf(parameters.get("REQUEST_SIZE"));
		String qpsRequestNumber = parameters.get("REQUEST_NUMBER") == null ? null
				: String.valueOf(parameters.get("REQUEST_NUMBER"));

		Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, parameters);
		try {
			if (krb_enabled) {
				BrokerUtil.authentication(this.conf, this.clusterConfig.getHbaseMasterPrincipal(),
						this.clusterConfig.getHbaseMasterUserKeytab());
			}

			this.connection = ConnectionFactory.createConnection(conf);
			Admin admin = this.connection.getAdmin();
			NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();
			namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxtables",
					quota.get(OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA));
			namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxregions",
					quota.get(OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA));
			admin.createNamespace(namespaceDescriptor);

			// for the namespace REQUEST_SIZE quota
			if (qpsRequestSize != null) {
				QuotaSettings sizeSettings = getQpsQuotaSettings(nsName, "REQUEST_SIZE", qpsRequestSize);
				if (sizeSettings != null) {
					admin.setQuota(sizeSettings);
				}
			}
			// for the namespace REQUEST_NUMBER quota
			if (qpsRequestNumber != null) {
				QuotaSettings numberSettings = getQpsQuotaSettings(nsName, "REQUEST_NUMBER", qpsRequestNumber);
				if (numberSettings != null) {
					admin.setQuota(numberSettings);
				}
			}

			admin.close();
		} catch (IOException e) {
			logger.error("HBase namespace create fail due to: " + e.getLocalizedMessage());
			//e.printStackTrace();
			throw e;
		} finally {
			this.connection.close();
		}
		return nsName;
	}

    @Override
    public String createPolicyForResources(String policyName, List<String> tableList, List<String> userList,
                                           String groupName, List<String> permissions){
        logger.info("Assign permissions [{}] to user [{}] of hbase namespace [{}].", permissions, userList, tableList);
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
        	if (krb_enabled) {
                BrokerUtil.authentication(
                        this.conf, this.clusterConfig.getHbaseMasterPrincipal(),
                        this.clusterConfig.getHbaseMasterUserKeytab());
			}

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
            //e.printStackTrace();
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
    public Map<String, Object> generateCredentialsInfo(String rawResourceName){
        return new HashMap<String, Object>(){
            {
                put("uri", "http://" + clusterConfig.getHbaseMaster() + ":" + clusterConfig.getHbaseRestPort());
                put("host", clusterConfig.getHbaseMaster());
                put("port", clusterConfig.getHbaseRestPort());
                put(OCDPConstants.HBASE_RESOURCE_TYPE, rawResourceName.replaceAll("-", ""));
            }
        };
    }

    @Override
    public  List<String> getResourceFromPolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, OCDPConstants.HBASE_RANGER_RESOURCE_TYPE);
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
		// support the Hbase qps quota
		String qpsRequestSize = cuzQuota.get("REQUEST_SIZE") == null ? null
				: String.valueOf(cuzQuota.get("REQUEST_SIZE"));
		String qpsRequestNumber = cuzQuota.get("REQUEST_NUMBER") == null ? null
				: String.valueOf(cuzQuota.get("REQUEST_NUMBER"));

        String serviceDefinitionId = instance.getServiceDefinitionId();
        String planId = instance.getPlanId();
        Map<String, String> quota = getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        String resourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        String ns = (String)instance.getServiceInstanceCredentials().get(resourceType);
        try{
        	if (krb_enabled) {
                BrokerUtil.authentication(conf, clusterConfig.getHbaseMasterPrincipal(),
                        clusterConfig.getHbaseMasterUserKeytab());
			}

            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(ns);
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxtables", quota.get(OCDPConstants.HBASE_NAMESPACE_TABLE_QUOTA));
            namespaceDescriptor.setConfiguration(
                    "hbase.namespace.quota.maxregions", quota.get(OCDPConstants.HBASE_NAMESPACE_REGION_QUOTA));
            admin.modifyNamespace(namespaceDescriptor);

			// for the namespace REQUEST_SIZE quota
			if (qpsRequestSize != null) {
				QuotaSettings sizeSettings = getQpsQuotaSettings(ns, "REQUEST_SIZE", qpsRequestSize);
				if (sizeSettings != null) {
					admin.setQuota(sizeSettings);
				}
			}
			// for the namespace REQUEST_NUMBER quota
			if (qpsRequestNumber != null) {
				QuotaSettings numberSettings = getQpsQuotaSettings(ns, "REQUEST_NUMBER", qpsRequestNumber);
				if (numberSettings != null) {
					admin.setQuota(numberSettings);
				}
			}

            admin.close();
        } catch (IOException e){
            logger.error("resizeResourceQuota() hit IOException: ", e);
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
