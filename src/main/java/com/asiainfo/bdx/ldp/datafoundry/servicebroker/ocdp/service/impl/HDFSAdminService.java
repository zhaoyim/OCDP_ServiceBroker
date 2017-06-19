package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CustomizeQuotaItem;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.cloud.servicebroker.model.Plan;
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

    private String serviceType = "HDFS";

    private String serviceResourceType = "HDFS Path";

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
            System.out.println("nameservices = " + nameservices);
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
                                     String bindingId, Map<String, Object> cuzQuota) throws Exception{
        String pathName = "/servicebroker/" + serviceInstanceId;
        Map<String, Long> quota = this.getQuotaFromPlan(serviceDefinitionId, planId, cuzQuota);
        this.createHDFSDir(pathName, quota.get("nameSpaceQuota"), quota.get("storageSpaceQuota"));
        return pathName;
    }

    public void createHDFSDir(String pathName, Long nameSpaceQuota, Long storageSpaceQuota) throws IOException{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if (! this.dfs.exists(new Path(pathName))){
                this.dfs.mkdirs(new Path(pathName), FS_PERMISSION);
                this.dfs.setQuota(new Path(pathName), nameSpaceQuota, storageSpaceQuota);
                logger.info("Create hdfs folder successful.");
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

    public void setQuota(String pathName, Long nameSpaceQuota, Long storageSpaceQuota) throws IOException {
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            this.dfs.setQuota(new Path(pathName), nameSpaceQuota, storageSpaceQuota);
        }catch (Exception e){
            logger.error("Set HDFS folder quota fails due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    @Override
    public String createPolicyForTenant(String policyName, List<String> resources, String tenantName, String groupName){
        logger.info("Assign read/write/execute permission to hdfs folder.");
        String policyId = null;
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(tenantName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("execute");}};
        ArrayList<String> conditions = new ArrayList<>();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is HDFS Policy", clusterConfig.getClusterName()+"_hadoop",true,true);
        rp.addResources2("path", resources,false,true);
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
        return rc.appendResourceToV2Policy(policyId, serviceInstanceResource, "path");
    }

    @Override
    public boolean appendUserToTenantPolicy(String policyId, String groupName, String accountName, List<String> permissions){
        return rc.appendUserToV2Policy(policyId, groupName, accountName, permissions);
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
    public boolean deletePolicyForTenant(String policyId){
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeResourceFromTenantPolicy(String policyId, String serviceInstanceResource){
        return rc.removeResourceFromV2Policy(policyId, serviceInstanceResource, "path");
    }

    @Override
    public boolean removeUserFromTenantPolicy(String policyId, String accountName){
        return rc.removeUserFromV2Policy(policyId, accountName);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String serviceInstanceId){
        return new HashMap<String, Object>(){
            {
                put("uri", webHdfsUrl + "/servicebroker/" + serviceInstanceId);
                put("host", clusterConfig.getHdfsNameNode());
                put("port", clusterConfig.getHdfsPort());
                put("HDFS Path", "/servicebroker/" + serviceInstanceId);
            }
        };
    }

    @Override
    public String getServiceResourceType(){
        return serviceResourceType;
    }

    @Override
    public String getServiceType(){
        return serviceType;
    }

    @Override
    public  List<String> getResourceFromTenantPolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, "path");
    }

    @Override
    public void resizeResourceQuota(String serviceInstanceId, Map<String, Object> cuzQuota){

    }

    private Map<String, Long> getQuotaFromPlan(String serviceDefinitionId, String planId, Map<String, Object> cuzQuota){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        Object customize = metadata.get("customize");
        long nameSpaceQuota, storageSpaceQuota;
        if(customize != null){
            // Customize quota case
            Map<String, Object> customizeMap = (Map<String,Object>)customize;

            CustomizeQuotaItem nameSpaceQuotaItem = (CustomizeQuotaItem)customizeMap.get("nameSpaceQuota");
            long defaultNameSpaceQuota = nameSpaceQuotaItem.getDefault();
            long maxNameSpaceQuota = nameSpaceQuotaItem.getMax();

            CustomizeQuotaItem storageSpaceQuotaItem = (CustomizeQuotaItem)customizeMap.get("storageSpaceQuota");
            long defaultStorageSpaceQuota = storageSpaceQuotaItem.getDefault();
            long maxStorageSpaceQuota = storageSpaceQuotaItem.getMax();

            logger.info(cuzQuota.toString());
            logger.info(cuzQuota.get("nameSpaceQuota") + " " + cuzQuota.get("storageSpaceQuota"));
            if (cuzQuota.get("nameSpaceQuota") != null && cuzQuota.get("storageSpaceQuota") != null){
                // customize quota have input value
                nameSpaceQuota = Long.parseLong((String)cuzQuota.get("nameSpaceQuota"));
                storageSpaceQuota =  Long.parseLong((String)cuzQuota.get("storageSpaceQuota"));
                // If customize quota exceeds plan limitation, use default value
                if(nameSpaceQuota > maxNameSpaceQuota){
                    nameSpaceQuota = defaultNameSpaceQuota;
                }
                if(storageSpaceQuota > maxStorageSpaceQuota){
                    storageSpaceQuota = defaultStorageSpaceQuota;
                }
            }else {
                // customize quota have not input value, use default value
                nameSpaceQuota = defaultNameSpaceQuota;
                storageSpaceQuota = defaultStorageSpaceQuota;
            }
        }else{
            // Non customize quota case, use plan.metadata.bullets
            List<String> bullets = (ArrayList)metadata.get("bullets");
            nameSpaceQuota = new Long(bullets.get(0).split(":")[1]);
            storageSpaceQuota = new Long(bullets.get(1).split(":")[1]);
        }
        Map<String, Long> quota = new HashMap<>();
        quota.put("nameSpaceQuota", nameSpaceQuota);
        quota.put("storageSpaceQuota", storageSpaceQuota * 1000000000);
        return quota;
    }
}
