package com.asiainfo.cm.servicebroker.dp.model;

import com.asiainfo.cm.servicebroker.dp.utils.OCDPConstants;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.*;

/**
 * Created by baikai on 8/3/21.
 * V2 API is compatible with Apache Ranger 5.0 +.
 * OCDP Service broker will only use V2 API.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RangerV2Policy{

    @JsonSerialize
    @JsonProperty("id")
    private String id;

    @JsonSerialize
    @JsonProperty("isEnabled")
    private boolean isEnabled;

    @JsonSerialize
    @JsonProperty("isAuditEnabled")
    private boolean isAuditEnabled;

    @JsonSerialize
    @JsonProperty("service")
    private String service;

    @JsonSerialize
    @JsonProperty("name")
    private String name;

    @JsonSerialize
    @JsonProperty("description")
    private String description;

    @JsonSerialize
    @JsonProperty("resources")
    private Map<String, RangerResource> resources;

    @JsonSerialize
    @JsonProperty("policyItems")
    private List<PolicyItem> policyItems;

    public RangerV2Policy(String policyName, String id, String description,
                            String service, boolean isEnabled, boolean isAuditEnabled){
        this.id = id;
        this.name = policyName;
        this.description = description;
        this.service = service;
        this.isAuditEnabled = isAuditEnabled;
        this.isEnabled = isEnabled;
        this.resources = new HashMap<>();
        this.policyItems = new ArrayList<>();
    }

    // For HBase/Hive ranger policy
    public void addResources(String resourceType, List<String> resourceList, boolean isExcludes){
        RangerResource rr = new RangerResource();
        rr.values.addAll(resourceList);
        rr.isExcludes = isExcludes;
        resources.put(resourceType, rr);
    }

    // For HDFS/Kafka/Yarn ranger policy
    public void addResources2(String resourceType, List<String> resourceList, boolean isExcludes, boolean isRecursive){
        RangerResource rr = new RangerResource();
        rr.values.addAll(resourceList);
        rr.isExcludes = isExcludes;
        rr.isRecursive = isRecursive;
        resources.put(resourceType, rr);
    }

    public void updateResource(String resourceType, String resourceName) {
        if (resourceType.equals(OCDPConstants.HDFS_RANGER_RESOURCE_TYPE) ||
                resourceType.equals(OCDPConstants.KAFKA_RESOURCE_TYPE) ||
                resourceType.equals(OCDPConstants.YARN_RANGER_RESOURCE_TYPE)){
            RangerResource rr = resources.get(resourceType);
            rr.values.add(resourceName);
        } else {
            RangerResource rr = resources.get(resourceType);
            rr.values.add(resourceName);
        }
    }

    public void updateUserAccesses(String userName, List<String> types){
        for(PolicyItem pi : policyItems){
            if(pi.getUsers().contains(userName)){
                pi.accesses.clear();
                pi.accesses.addAll(pi.getAccesses(types));
                break;
            }
        }
    }

    public void removeResource(String resourceType, String resourceName) {
        RangerResource rr = resources.get(resourceType);
        rr.values.remove(resourceName);
    }

    public void addPolicyItems(List<String> users, List<String> groups, List<String> conditions,
                               boolean delegateAdmin, List<String> types){
        for(String user : users){
            addPolicyItem(user, groups, conditions, delegateAdmin, types);
        }
    }

    public void addPolicyItem(String user, List<String> groups, List<String> conditions,
                               boolean delegateAdmin, List<String> types) {
        PolicyItem pi = new PolicyItem();
        pi.delegateAdmin = delegateAdmin;
        pi.users.add(user);
        //Temp fix for citic case, do not pass group when create policy
        //pi.groups.addAll(groups);
        pi.conditions.addAll(conditions);
        pi.accesses = pi.getAccesses(types);

        policyItems.add(pi);
    }

    public void removePolicyItem(String user) {
        for(PolicyItem pi : policyItems){
            if(pi.getUsers().contains(user)){
                policyItems.remove(pi);
                break;
            }
        }
    }

    public String getPolicyId(){return id;}
    public String getPolicyName(){return name;}
    public List<PolicyItem> getPolicyItems(){return policyItems;}
    public List<String> getUserList(){
        List<String> users = new LinkedList<>();
        for (PolicyItem policyItem: policyItems) {
            users.addAll(policyItem.getUsers());
        }
        return users;
    }
    //Temp fix for citic case, do not pass group when create policy
    /**
    public List<String> getGroupList(){
        return this.policyItems.get(0).getGroups();
    }
     **/
    public List<String> getResourceValues(String resourceType){
        return resources.get(resourceType).values;
    }

    class RangerResource{
        boolean isExcludes;
        boolean isRecursive;
        List<String> values = new ArrayList<String>();
    }

    class PolicyItem{
        List<String> users = new ArrayList<String>();
        //List<String> groups = new ArrayList<String>();
        boolean delegateAdmin;
        List<RangerAccess> accesses = new ArrayList<RangerAccess>();
        List<String> conditions = new ArrayList<String>();

        private class RangerAccess {
            boolean isAllowed;
            String type;

            RangerAccess(boolean isAllowed, String type){
                this.isAllowed = isAllowed;
                this.type = type;
            }
        }

        public List<RangerAccess> getAccesses(List<String> types){

            List<RangerAccess> accesses = new ArrayList<RangerAccess>();
            int elementNum = 0;
            while(elementNum < types.size()){
                accesses.add(new RangerAccess(true,types.get(elementNum)));
                elementNum++;
            }
            return accesses;
        }

        public List<String> getUsers(){
            return users;
        }

        //Temp fix for citic case, do not pass group when create policy
        /**
        public List<String> getGroups(){
            return groups;
        }
         **/
    }

}
