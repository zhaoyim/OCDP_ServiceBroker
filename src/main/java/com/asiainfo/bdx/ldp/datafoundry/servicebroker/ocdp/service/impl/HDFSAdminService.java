package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HDFSAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    private static final FsPermission FS_PERMISSION = new FsPermission(FsAction.ALL, FsAction.ALL,
    		 FsAction.ALL);

    private static final List<String> ACCESSES = Lists.newArrayList("read", "write", "execute");

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private DistributedFileSystem dfs;

    private Configuration conf;

    private String hdfsRPCUrl;

    private static final String HDFS_NAME_SPACE_QUOTA = "1000";

    private static final String HDFS_STORAGE_SPACE_QUOTA = "1000000000";
    
    private boolean krb_enabled;

    @Autowired
    public HDFSAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.dfs = new DistributedFileSystem();

        this.conf = new Configuration();

        if (this.clusterConfig.getHdfsNameservices() != null) {
            String nameservices = this.clusterConfig.getHdfsNameservices();
            String[] namenodesAddr = {this.clusterConfig.getHdfs_nameNode1_addr(), this.clusterConfig.getHdfs_nameNode2_addr()};
            String[] namenodes = {this.clusterConfig.getHdfs_nameNode1(), this.clusterConfig.getHdfs_nameNode2()};
            conf.set("fs.defaultFS", "hdfs://" + nameservices);
            conf.set("dfs.nameservices", nameservices);
            conf.set("dfs.ha.namenodes." + nameservices, namenodes[0] + "," + namenodes[1]);
            conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[0], namenodesAddr[0]);
            conf.set("dfs.namenode.rpc-address." + nameservices + "." + namenodes[1], namenodesAddr[1]);
            conf.set("dfs.client.failover.proxy.provider." + nameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            this.hdfsRPCUrl = "hdfs://" + this.clusterConfig.getHdfsNameservices() + ":" + this.clusterConfig.getHdfsRpcPort();
        }
        else {
            this.hdfsRPCUrl = "hdfs://" + this.clusterConfig.getHdfsNameNode() + ":" + this.clusterConfig.getHdfsRpcPort();
        }
        
        this.krb_enabled = this.clusterConfig.krbEnabled();
        logger.info("Kerberos enabled: " + this.krb_enabled);
        
        if (krb_enabled) {
            conf.set("hadoop.security.authentication", "Kerberos");
            conf.set("hdfs.kerberos.principal", this.clusterConfig.getHdfsSuperUser());
            conf.set("hdfs.keytab.file", this.clusterConfig.getHdfsUserKeytab());
            System.setProperty("java.security.krb5.conf", this.clusterConfig.getKrb5FilePath());
		}
    }



    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> parameters) throws Exception{
        String pathName = parameters.get("cuzBsiName") == null ? serviceInstanceId : String.valueOf(parameters.get("cuzBsiName"));
        
//        String pathName = "/servicebroker/" + serviceInstanceId;
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, parameters);
        this.createHDFSDir(
                pathName, quota.get(OCDPConstants.HDFS_NAMESPACE_QUOTA), quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
        return pathName;
    }

    public void createHDFSDir(String pathName, String nameSpaceQuota, String storageSpaceQuota) throws IOException{
        try{
        	if (krb_enabled) {
                BrokerUtil.authentication(
                        this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
			}

            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if (! this.dfs.exists(new Path(pathName))){
                this.dfs.mkdirs(new Path(pathName), FS_PERMISSION);
                if (nameSpaceQuota != null && storageSpaceQuota !=null) {
                    this.dfs.setQuota(new Path(pathName),
                            Long.parseLong(nameSpaceQuota), Long.parseLong(storageSpaceQuota));
                    logger.info("Set path " + pathName + "'s namespace quota to " + nameSpaceQuota +
                            ", set storage space quota to " + storageSpaceQuota + ".");
                }
                logger.info("Create hdfs folder " + pathName + " successful.");
            } else {
                logger.info("HDFS folder exists, not need to create again.");
            }
        }catch (Exception e){
            logger.error("Set HDFS folder quota fails due to: " + e.getLocalizedMessage());
            //e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    public void setQuota(String pathName, String nameSpaceQuota, String storageSpaceQuota) throws IOException {
        try{
        	if (krb_enabled) {
                BrokerUtil.authentication(
                        this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
			}
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if(nameSpaceQuota == null || nameSpaceQuota.equals("")){
                // Use default namespacequota if not pass.
                nameSpaceQuota = HDFS_NAME_SPACE_QUOTA;
            }
            if (storageSpaceQuota == null || storageSpaceQuota.equals("")){
                // Use default storgespacequota if not pass.
                storageSpaceQuota = HDFS_STORAGE_SPACE_QUOTA;
            }
            this.dfs.setQuota(new Path(pathName), Long.parseLong(nameSpaceQuota), Long.parseLong(storageSpaceQuota));
        }catch (Exception e){
            logger.error("Set HDFS folder quota fails due to: " + e.getLocalizedMessage());
            //e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
            logger.info("Set path " + pathName + "'s namespace quota to " + nameSpaceQuota +
                    ", set storage space quota to " + storageSpaceQuota + ".");
        }
    }
    
    @Override
    public String createPolicyForResources(String policyName, List<String> resources, List<String> userList,
                                           String groupName, List<String> permissions){
        String policyId = null;
        String serviceName = clusterConfig.getClusterName()+"_hadoop";
        ArrayList<String> groupList = Lists.newArrayList(groupName);
        //ArrayList<String> userList = new ArrayList<String>(){{add(userName);}};
        //ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("execute");}};
        ArrayList<String> conditions = Lists.newArrayList();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is HDFS Policy",serviceName,true,true);
        rp.addResources2(OCDPConstants.HDFS_RANGER_RESOURCE_TYPE, resources,false,true);
        if (permissions == null){
            rp.addPolicyItems(userList,groupList,conditions,false,ACCESSES);
        } else {
            rp.addPolicyItems(userList,groupList,conditions,false,permissions);
        }
        String newPolicyString = rc.createV2Policy(serviceName, rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        else {
            logger.error("Failed to create hdfs policy to user [{}] of resources [{}] ", userList.toString(), resources);
        }
        return policyId;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        return rc.appendResourceToV2Policy(policyId, serviceInstanceResource, OCDPConstants.HDFS_RANGER_RESOURCE_TYPE);
    }

    @Override
    public boolean appendUsersToPolicy(String policyId, String groupName, List<String> users, List<String> permissions){
        return rc.appendUsersToV2Policy(policyId, groupName, users, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
        	if (krb_enabled) {
                BrokerUtil.authentication(
                        this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
			}

            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            this.dfs.delete(new Path(serviceInstanceResuorceName), true);
            logger.info("Delete hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder delete fail due to: " + e.getLocalizedMessage());
            //e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    @Override
    public boolean deletePolicyForResources(String policyId){
        if (rc.getV2PolicyById(policyId) != null) {
            logger.info("Unassign read/write/execute permission to hdfs folder.");
            return this.rc.removeV2Policy(policyId);
        }
        else {
            logger.warn("ranger policy " + policyId + " doesn't exist, do not need to remove");
            return true;
        }
    }

    @Override
    public boolean removeResourceFromPolicy(String policyId, String serviceInstanceResource){
        return rc.removeResourceFromV2Policy(policyId, serviceInstanceResource, OCDPConstants.HDFS_RANGER_RESOURCE_TYPE);
    }

    @Override
    public boolean removeUserFromPolicy(String policyId, String userName){
        return rc.removeUserFromV2Policy(policyId, userName);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String resourceName){
        if (clusterConfig.getHdfsNameservices() == null){
            // non-HA
            return new HashMap<String, Object>(){
                {
                    put("uri",
                            "http://" + clusterConfig.getHdfsNameNode() + ":" + clusterConfig.getHdfsPort() + "/webhdfs/v1");
                    put("host", clusterConfig.getHdfsNameNode());
                    put("port", clusterConfig.getHdfsPort());
                    put(OCDPConstants.HDFS_RESOURCE_TYPE, resourceName);
                }
            };
        } else {
            // HA
            String hdfsNameNode1 = clusterConfig.getHdfs_nameNode1();
            String hdfsNameNode2 = clusterConfig.getHdfs_nameNode2();
            String hdfsPort = clusterConfig.getHdfsPort();
            return new HashMap<String, Object>(){
                {
                    put("uri1",
                            "http://" + hdfsNameNode1 + ":" + hdfsPort + "/webhdfs/v1");
                    put("uri2",
                            "http://" + hdfsNameNode2 + ":" + hdfsPort + "/webhdfs/v1");
                    put("nameservice", clusterConfig.getHdfsNameservices());
                    put("namenode1", hdfsNameNode1);
                    put("namenode2", hdfsNameNode2);
                    put("namenode1 address", clusterConfig.getHdfs_nameNode1_addr());
                    put("namenode2 address", clusterConfig.getHdfs_nameNode2_addr());
                    put(OCDPConstants.HDFS_RESOURCE_TYPE, resourceName);
                }
            };
        }
    }

    @Override
    public  List<String> getResourceFromPolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, OCDPConstants.HDFS_RANGER_RESOURCE_TYPE);
    }

    @Override
    public void resizeResourceQuota(ServiceInstance instance, Map<String, Object> cuzQuota) throws IOException{
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String planId = instance.getPlanId();
        Map<String, String> quota = getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
		OCDPAdminServiceMapper mapper = (OCDPAdminServiceMapper) this.context.getBean("OCDPAdminServiceMapper");
        String resourceType = mapper.getOCDPResourceType(serviceDefinitionId);
        String path = (String)instance.getServiceInstanceCredentials().get(resourceType);
        try{
        	// Construct hive database path for hive case
	        if(resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
	            path = "/apps/hive/warehouse/" + path.split(":")[0] + ".db";
	            setQuota(path, "-1", quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
	            return;
	        }

            setQuota(path, quota.get(OCDPConstants.HDFS_NAMESPACE_QUOTA), quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
        } catch (IOException e){
            logger.error("resizeResourceQuota() hit IOException: ", e);
            throw e;
        }
    }
    
    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
    }

}
