package com.asiainfo.cm.servicebroker.dp.repository;

import com.asiainfo.cm.servicebroker.dp.model.ServiceInstance;

/**
 * Repository for ServiceInstance objects
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPServiceInstanceRepository {

    ServiceInstance findOne(String serviceInstanceId);

    void save(ServiceInstance instance);

    void delete(String serviceInstanceId);

}