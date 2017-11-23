package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CapacitySchedulerConfig;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Aaron on 16/7/26.
 */
public class YarnCapacityCalculator {

    private Logger logger = LoggerFactory.getLogger(YarnCapacityCalculator.class);
    private Map<String,String> properties;
    private String allQueues;
    private Double totalMemory;
    private Double availableCapacity;


    public YarnCapacityCalculator(String totalMem, CapacitySchedulerConfig csConfig){

        this.properties = csConfig.getItems().get(0).getProperties();
        this.totalMemory = Double.parseDouble(totalMem);
        this.availableCapacity = Double.parseDouble(properties.get("yarn.scheduler.capacity.root.default.capacity"));
        this.allQueues =  properties.get("yarn.scheduler.capacity.root.queues");
    }

    public Map<String,String> getProperties(){ return properties;}

    /**
     * A Method to apply queue with capacity quota
     * @param quota
     * @param queueName
     */
    public String applyQueue(Long quota, String queueName){
        // To make sure that the sum of all the queues capacity equals 100%, the patch for ambari server to support
        // two decimal places in capacity scheduler should be installed.
        String targetQueueCapacity = String.format("%.2f", ((100 * quota) / (totalMemory / 1024)));
        String resourcePoolCapacity = String.format("%.2f", (availableCapacity - (Double.parseDouble(targetQueueCapacity))));
        if(Double.parseDouble(targetQueueCapacity) > availableCapacity){
            return null;
        }
        // Get latest value of prop yarn.scheduler.capacity.root.queues
        this.allQueues =  properties.get("yarn.scheduler.capacity.root.queues");
        // Queue creation logic:
        // 1) use queue name that assigned by ocmanager
        // 2) get first empty queue if ocmaneger not assign queue name
        // 3) create random queue name if no empty queue available
        if(queueName != null){
            setNewQueueCapacity(queueName, targetQueueCapacity);
        } else {
            String emptyQueue = getFirstEmptyQueue();
            if(emptyQueue != null) {
                resetQueueCapacity(emptyQueue, "0", targetQueueCapacity);
                queueName = emptyQueue;
            } else {
                String newQueue = UUID.randomUUID().toString();
                setNewQueueCapacity(newQueue, targetQueueCapacity);
                queueName = newQueue;
            }
        }
        resetQueueCapacity("root.default", null, resourcePoolCapacity);
        return queueName;
    }

    public void updateQueue(String queueName, Long quota) {
        // Re-calculate new capacity
        String originQueueCapacity = properties.get("yarn.scheduler.capacity."+
                queueName+".capacity");
        String targetQueueCapacity = String.format("%.2f", ((100 * quota) / (totalMemory / 1024)));
        String resourcePoolCapacity = String.format("%.2f",
                (availableCapacity - (Double.parseDouble(targetQueueCapacity) - Double.parseDouble(originQueueCapacity))));
        // Update queue
        resetQueueCapacity(queueName, null, targetQueueCapacity);
        // Update default queue
        resetQueueCapacity("root.default", null, resourcePoolCapacity);
    }

    /**
     * remoke capacity to zero
     */
    public boolean revokeQueue(String serviceInstanceResuorceName){

        String targetQueueCapacity = properties.get("yarn.scheduler.capacity."+
                serviceInstanceResuorceName+".capacity");
        String resourcePoolCapacity = String.valueOf(availableCapacity+Double.parseDouble(targetQueueCapacity));
        if(targetQueueCapacity == null)
            return false;
        else {
            // Set queue capacity to zero
            resetQueueCapacity(serviceInstanceResuorceName, targetQueueCapacity, "0");
            // Update defauly queue
            resetQueueCapacity("root.default", null, resourcePoolCapacity);
        }
        return true;
    }

    private String getFirstEmptyQueue(){
        for(String queue : Splitter.on(",").split(allQueues))
        {
            if(properties.get("yarn.scheduler.capacity.root."+queue+".capacity").equals("0")
                    &&properties.get("yarn.scheduler.capacity.root."+queue+".maximum-capacity").equals("0"))
            {
                return queue;
            }
        }

        return null;

    }

    public String removeQueueMapping(String user, String queue){
        String absoluteQueue = queue.substring(5);
        String newQueueMapStr = "";
        String queueMapStr = this.properties.get("yarn.scheduler.capacity.queue-mappings");
        if(queueMapStr == null)
            return null;
        for(String queueMap : Splitter.on(",").split(queueMapStr))
        {
            if(queueMap.startsWith("u")&&queueMap.endsWith(absoluteQueue)){
                if(queueMap.contains(user)){
                    continue;
                }
            }
            newQueueMapStr += queueMap;
            newQueueMapStr += ",";
        }
        if(!newQueueMapStr.equals("")){
            newQueueMapStr = newQueueMapStr.substring(0,newQueueMapStr.length()-1);
            properties.replace("yarn.scheduler.capacity.queue-mappings",queueMapStr,newQueueMapStr);
        }else{
            properties.replace("yarn.scheduler.capacity.queue-mappings",queueMapStr,"");
        }
        return newQueueMapStr;

    }

    public String removeQueueMapping(String queue){
        String absoluteQueue = queue.substring(5);
        String newQueueMapStr = "";
        String queueMapStr = this.properties.get("yarn.scheduler.capacity.queue-mappings");
        if(queueMapStr == null)
            return null;
        for(String queueMap : Splitter.on(",").split(queueMapStr))
        {
            if(queueMap.startsWith("u")&&queueMap.endsWith(absoluteQueue)){
                continue;
            }
            newQueueMapStr += queueMap;
            newQueueMapStr += ",";
        }
        if(!newQueueMapStr.equals("")){
            newQueueMapStr = newQueueMapStr.substring(0,newQueueMapStr.length()-1);
            properties.replace("yarn.scheduler.capacity.queue-mappings",queueMapStr,newQueueMapStr);
        }else{
            properties.replace("yarn.scheduler.capacity.queue-mappings",queueMapStr,"");
        }
        return newQueueMapStr;
    }


    public String addQueueMapping(String user, String queue){
        String absoluteQueueName = queue.substring(5);
        String newQueueMapStr;
        String queueMapStr = this.properties.get("yarn.scheduler.capacity.queue-mappings");

        if(queueMapStr == null||queueMapStr.equals(""))
            newQueueMapStr = "u:"+user+":"+absoluteQueueName;
        else
            newQueueMapStr = queueMapStr + ",u:" +user+ ":" +absoluteQueueName;

        properties.put("yarn.scheduler.capacity.queue-mappings",newQueueMapStr);

        return newQueueMapStr;
    }
    
    private void resetQueueCapacity(String queueName, String oldQueueCapacity, String targetQueueCapacity) {
        if (oldQueueCapacity == null){
            properties.replace("yarn.scheduler.capacity."+queueName+".capacity", targetQueueCapacity);
            properties.replace("yarn.scheduler.capacity."+queueName+".maximum-capacity",targetQueueCapacity);
        } else {
            properties.replace("yarn.scheduler.capacity."+queueName+".capacity",oldQueueCapacity, targetQueueCapacity);
            properties.replace("yarn.scheduler.capacity."+queueName+".maximum-capacity",oldQueueCapacity,targetQueueCapacity);
        }
    }

    private void setNewQueueCapacity(String queueName, String targetQueueCapacity) {
    	if (allQueues.indexOf(queueName) < 0) {
            properties.replace("yarn.scheduler.capacity.root.queues",allQueues,allQueues+","+queueName);
		}
        properties.put("yarn.scheduler.capacity.root."+queueName+".capacity",targetQueueCapacity);
        properties.put("yarn.scheduler.capacity.root."+queueName+".maximum-capacity",targetQueueCapacity);
    }

}
