package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
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

    @Autowired
    public HDFSAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.dfs = new DistributedFileSystem();

        this.conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", clusterConfig.getHdfsUserKeytab());

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.hdfsRPCUrl = "hdfs://" + this.clusterConfig.getHdfsNameNode() + ":" + this.clusterConfig.getHdfsRpcPort();
        this.webHdfsUrl = "http://" + this.clusterConfig.getHdfsNameNode() + ":" + this.clusterConfig.getHdfsPort() + "/webhdfs/v1";
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     String bindingId, String accountName, Map<String, Object> cuzQuota) throws Exception{
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
    public String assignPermissionToResources(String policyName, List<String> resources, String accountName, String groupName){
        logger.info("Assign read/write/execute permission to hdfs folder.");
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("execute");}};
        ArrayList<String> conditions = new ArrayList<>();
        return this.rc.createHDFSPolicy(policyName,"This is HDFS Policy",clusterConfig.getClusterName()+"_hadoop",
                resources, groupList, userList,types,conditions);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, true);
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
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, false);
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>(){
            {
                put("uri", webHdfsUrl + serviceInstanceResource);
                put("username", accountName + "@" + clusterConfig.getKrbRealm());
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getHdfsNameNode());
                put("port", clusterConfig.getHdfsPort());
                put("name", serviceInstanceResource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

    @Override
    public Map<String, String> getCredentialsInfo(String serviceInstanceId, String accountName, String password){
        return new HashMap<String, String>(){
            {
                put("username", accountName + "@" + clusterConfig.getKrbRealm());
                put("password", password);
                put("uri", webHdfsUrl + "/servicebroker/" + serviceInstanceId);
                put("host", clusterConfig.getHdfsNameNode());
                put("port", clusterConfig.getHdfsPort());
                put("HDFS Path", "/servicebroker/" + serviceInstanceId);
            }
        };
    }

    private boolean updateUserForResourcePermission(String policyId, String groupName, String accountName, boolean isAppend){
        String currentPolicy = this.rc.getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.updatePolicy(
                groupName, accountName, new ArrayList<String>(){{add("read");add("write");add("execute");}}, isAppend);
        return this.rc.updateV2Policy(policyId, gson.toJson(rp));
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
            logger.info((String) cuzQuota.get("nameSpaceQuota") + " " + (String) cuzQuota.get("storageSpaceQuota") );
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
