package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by baikai on 8/8/16.
 */
@Service
public class HiveCommonService {

    private Logger logger = LoggerFactory.getLogger(HiveCommonService.class);

    static final Gson gson = new GsonBuilder().create();

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private Configuration conf;

    private Connection conn;

    private String hiveJDBCUrl;

    @Autowired
    public HiveCommonService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.hiveJDBCUrl = "jdbc:hive2://" + this.clusterConfig.getHiveHost() + ":" + this.clusterConfig.getHivePort() +
                "/default;principal=" + this.clusterConfig.getHiveSuperUser();
    }

    public String createDatabase(String serviceInstanceId) throws Exception{
        String databaseName = serviceInstanceId.replaceAll("-", "");
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHiveSuperUser(), this.clusterConfig.getHiveSuperUserKeytab());
            Class.forName(driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl);
            Statement stmt = conn.createStatement();
            stmt.execute("create database " + databaseName);
        }catch (ClassNotFoundException e){
            logger.error("Hive JDBC driver not found in classpath.");
            e.printStackTrace();
            throw e;
        }
        catch(SQLException e){
            logger.error("Hive database create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
            logger.info("Hive Database " + databaseName + " has been created.");
        }
        return databaseName;
    }

    public String assignPermissionToDatabase(String policyName, final String dbName, String userName, String groupName){
        logger.info("Assigning select/update/create/drop/alter/index/lock/all permission to hive database.");
        String policyId = null;
        ArrayList<String> dbList = new ArrayList<String>(){{add(dbName);}};
        ArrayList<String> tbList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> cList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(userName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("select"); add("update");
            add("create"); add("drop"); add("alter"); add("index"); add("lock"); add("all");}};
        ArrayList<String> conditions = new ArrayList<String>();
        RangerV2Policy rp = new RangerV2Policy(
                policyName,"","This is Hive Policy",clusterConfig.getClusterName()+"_hive",true,true);
        rp.addResources(OCDPConstants.HIVE_RANGER_RESOURCE_TYPE, dbList, false);
        rp.addResources("table", tbList, false);
        rp.addResources("column", cList, false);
        rp.addPolicyItems(userList,groupList,conditions,false,types);
        String newPolicyString = rc.createV2Policy(rp);
        if (newPolicyString != null){
            RangerV2Policy newPolicyObj = gson.fromJson(newPolicyString, RangerV2Policy.class);
            policyId = newPolicyObj.getPolicyId();
            logger.info("Assign permissions [{}] of user [{}] to hive database [{}] successful with policyid [{}].", types, userName, dbName, policyId);
            return policyId;
        }
        logger.error("Assign permissions of user [{}] to hive database [{}] failed!", userName, dbName);
        return policyId;
    }

    public boolean appendResourceToDatabasePermission(String policyId, String databaseName){
        return rc.appendResourceToV2Policy(policyId, databaseName, OCDPConstants.HIVE_RANGER_RESOURCE_TYPE);
    }

    public boolean appendUserToDatabasePermission(
            String policyId, String groupName, String userName, List<String> permissions) {
        return rc.appendUserToV2Policy(policyId, groupName, userName, permissions);
    }

    public void deleteDatabase(String dbName) throws Exception{
        try{
            BrokerUtil.authentication(
                    this.conf, this.clusterConfig.getHiveSuperUser(), this.clusterConfig.getHiveSuperUserKeytab());
            Class.forName(driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbName + " cascade");
        }catch (ClassNotFoundException e){
            logger.error("Hive JDBC driver not found in classpath.");
            e.printStackTrace();
            throw e;
        }catch (SQLException e){
            logger.error("Hive database drop fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
        }
    }

    public boolean unassignPermissionFromDatabase(String policyId){
        return rc.removeV2Policy(policyId);
    }

    public boolean removeResourceFromDatabasePermission(String policyId, String databaseName){
        return rc.removeResourceFromV2Policy(policyId, databaseName, OCDPConstants.HIVE_RANGER_RESOURCE_TYPE);
    }

    public boolean removeUserFromDatabasePermission(String policyId, String userName){
        return rc.removeUserFromV2Policy(policyId, userName);
    }

    public  List<String> getResourceFromDatabasePolicy(String policyId){
        return rc.getResourcsFromV2Policy(policyId, OCDPConstants.HIVE_RANGER_RESOURCE_TYPE);
    }

}
