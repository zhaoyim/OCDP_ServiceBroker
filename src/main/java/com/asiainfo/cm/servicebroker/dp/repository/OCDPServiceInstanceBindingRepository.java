package com.asiainfo.cm.servicebroker.dp.repository;

import com.asiainfo.cm.servicebroker.dp.model.ServiceInstanceBinding;

/**
 * Repository for ServiceInstanceBinding objects
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPServiceInstanceBindingRepository {

    ServiceInstanceBinding findOne(String serviceInstanceId, String bindingId);

    void save(ServiceInstanceBinding binding);

    void delete(String serviceInstanceId, String bindingId);

}
