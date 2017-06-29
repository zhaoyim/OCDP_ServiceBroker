package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HDFSAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    private static final FsPermission FS_PERMISSION = new FsPermission(FsAction.ALL, FsAction.ALL,
            FsAction.NONE);

    private static final FsPermission FS_USER_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE,
            FsAction.NONE);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private DistributedFileSystem dfs;

    private Configuration conf;

    private String hdfsRPCUrl;

    private String webHdfsUrl;

    private static final String HDFS_NAME_SPACE_QUOTA = "1000";

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


        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", clusterConfig.getHdfsUserKeytab());

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.webHdfsUrl = "http://" + this.clusterConfig.getHdfsNameNode() + ":" + this.clusterConfig.getHdfsPort() + "/webhdfs/v1";
    }



    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     Map<String, Object> cuzQuota) throws Exception{
        String pathName = "/servicebroker/" + serviceInstanceId;
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        this.createHDFSDir(
                pathName, quota.get(OCDPConstants.HDFS_NAMESPACE_QUOTA), quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
        return pathName;
    }

    public void createHDFSDir(String pathName, String nameSpaceQuota, String storageSpaceQuota) throws IOException{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if (! this.dfs.exists(new Path(pathName))){
                this.dfs.mkdirs(new Path(pathName), FS_PERMISSION);
                if (nameSpaceQuota != null && storageSpaceQuota !=null) {
                    this.dfs.setQuota(new Path(pathName),
                            Long.parseLong(nameSpaceQuota), Long.parseLong(storageSpaceQuota));
                }
                logger.info("Create hdfs folder " + pathName + " successful.");
            } else {
                logger.info("HDFS folder exists, not need to create again.");
            }
        }catch (Exception e){
            logger.error("Set HDFS folder quota fails due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    public void setQuota(String pathName, String nameSpaceQuota, String storageSpaceQuota) throws IOException {
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if(nameSpaceQuota == null || nameSpaceQuota.equals("")){
                // Use default namespacequota if not pass.
                nameSpaceQuota = HDFS_NAME_SPACE_QUOTA;
            }
            this.dfs.setQuota(new Path(pathName), Long.parseLong(nameSpaceQuota), Long.parseLong(storageSpaceQuota));
        }catch (Exception e){
            logger.error("Set HDFS folder quota fails due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    @Override
    public String createPolicyForResources(String policyName, List<String> resources, String userName, String groupName){
        String policyId = null;
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(userName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("execute");}};
        ArrayList<String> conditions = new ArrayList<>();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is HDFS Policy", clusterConfig.getClusterName()+"_hadoop",true,true);
        rp.addResources2(OCDPConstants.HDFS_RANGER_RESOURCE_TYPE, resources,false,true);
        rp.addPolicyItems(userList,groupList,conditions,false,types);
        String newPolicyString = rc.createV2Policy(rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
        }
        else {
            logger.error("Failed to create hdfs policy to user [{}] of resources [{}] ", userName, resources);
        }
        return policyId;
    }

    @Override
    public boolean appendResourcesToPolicy(String policyId, String serviceInstanceResource){
        return rc.appendResourceToV2Policy(policyId, serviceInstanceResource, OCDPConstants.HDFS_RANGER_RESOURCE_TYPE);
    }

    @Override
    public boolean appendUserToPolicy(String policyId, String groupName, String userName, List<String> permissions){
        return rc.appendUserToV2Policy(policyId, groupName, userName, permissions);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            this.dfs.delete(new Path(serviceInstanceResuorceName), true);
            logger.info("Delete hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder delete fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    @Override
    public boolean deletePolicyForResources(String policyId){
        if (rc.getV2Policy(policyId) != null) {
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
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        return new HashMap<String, Object>(){
            {
                put("uri", webHdfsUrl + "/servicebroker/" + serviceInstanceId);
                put("host", clusterConfig.getHdfsNameNode());
                put("port", clusterConfig.getHdfsPort());
//                put(OCDPConstants.HDFS_RESOURCE_TYPE, "/servicebroker/" + serviceInstanceId);
            }
        };
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
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String path = (String)instance.getServiceInstanceCredentials().get(resourceType);
        // Construct hive database path for hive case
        if(resourceType.equals(OCDPConstants.HIVE_RESOURCE_TYPE)){
            path = "/apps/hive/warehouse/" + path.split(":")[0] + ".db";
        }
        try{
            setQuota(path, quota.get(OCDPConstants.HDFS_NAMESPACE_QUOTA), quota.get(OCDPConstants.HDFS_STORAGE_QUOTA));
        } catch (IOException e){
            e.printStackTrace();
            throw e;
        }
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        return catalogConfig.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
    }

}
