package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import java.io.IOException;
import java.lang.Thread;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.Future;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import org.springframework.cloud.servicebroker.model.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OCDPCreateServiceInstanceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;

/**
 * OCDP impl to manage hadoop service instances.  Creating a service does the following:
 * creates a new service instance user,
 * create hadoop service instance(e.g. hdfs dir, hbase table...),
 * set permission for service instance user,
 * saves the ServiceInstance info to the hdaoop repository.
 *  
 * @author whitebai1986@gmail.com
 */
@Service
public class OCDPServiceInstanceCommonService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceCommonService.class);

    @Autowired
	private OCDPServiceInstanceRepository repository;

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private LdapTemplate ldap;

    private krbClient kc;

    private etcdClient etcdClient;

    private rangerClient rc;

    @Autowired
    public OCDPServiceInstanceCommonService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.kc = new krbClient(clusterConfig);
        this.etcdClient = clusterConfig.getEtcdClient();
        this.rc = clusterConfig.getRangerClient();
    }

    @Async
    public Future<CreateServiceInstanceResponse> doCreateServiceInstanceAsync(
            CreateServiceInstanceRequest request) throws OCDPServiceException {
        return new AsyncResult<CreateServiceInstanceResponse>(
                doCreateServiceInstance(request)
        );
    }

    public CreateServiceInstanceResponse doCreateServiceInstance(
            CreateServiceInstanceRequest request) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String planId = request.getPlanId();
        Map<String, Object> params = request.getParameters();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // 1) Create big data resources like hdfs folder, hbase namespace ...
        String serviceInstanceResource = createTenentResource(
                ocdp, serviceDefinitionId, planId, serviceInstanceId, params);

        // 2) Generate service instance credential info
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(serviceInstanceId);
        String serviceResourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        // For spark/mr instance provision, need append queue name into credentials,
        // because function generateCredentialsInfo not append it
        if(! credentials.containsKey(serviceResourceType))
            credentials.put(serviceResourceType, serviceInstanceResource);

        // 3) Save service instance
        ServiceInstance instance = new ServiceInstance(request);
        instance.setCredential(credentials);
        repository.save(instance);

        CreateServiceInstanceResponse response = new OCDPCreateServiceInstanceResponse()
                .withCredential(credentials)
                .withAsync(false);
        logger.info("Create service instance " + serviceInstanceId + " successfully!");
        return response;
    }

    @Async
    public Future<DeleteServiceInstanceResponse> doDeleteServiceInstanceAsync(
            DeleteServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        return new AsyncResult<DeleteServiceInstanceResponse>(doDeleteServiceInstance(request, instance));
    }

    public DeleteServiceInstanceResponse doDeleteServiceInstance(
            DeleteServiceInstanceRequest request, ServiceInstance instance) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        String serviceInstanceId = request.getServiceInstanceId();
        Map<String, Object> Credential = instance.getServiceInstanceCredentials();
        String serviceResourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String serviceInstanceResource = (String)Credential.get(serviceResourceType);
        String serviceInstancePolicyId = (String)Credential.get("rangerPolicyId");
        // 1) Remove resource from ranger policy if it exists
        if (serviceInstancePolicyId != null && serviceInstancePolicyId.length() != 0 ) {
        logger.info("Service instance policy exists, start to deleting policy " + serviceInstancePolicyId);
            if (!ocdp.deletePolicyForResources(serviceInstancePolicyId)) {
            	logger.error("Ranger policy [{}] delete failed.", serviceInstancePolicyId);
                throw new OCDPServiceException("Ranger policy delete failed.");
            }
        }
        // 2 )Delete big data resources like hdfs folder, hbase namespace ...
        deleteTenentResource(ocdp, serviceInstanceResource);

        // 3) Clean service instance from etcd
        repository.delete(serviceInstanceId);
        logger.info("Delete service instance " + serviceInstanceId + " successfully!");

        return new DeleteServiceInstanceResponse().withAsync(false);
	}

    @Async
    public Future<UpdateServiceInstanceResponse> doUpdateServiceInstanceAsync(
            UpdateServiceInstanceRequest request, ServiceInstance instance, String password) throws OCDPServiceException {
        return new AsyncResult<UpdateServiceInstanceResponse>(doUpdateServiceInstance(request, instance, password));
    }

    @SuppressWarnings("unchecked")
    public UpdateServiceInstanceResponse doUpdateServiceInstance(
            UpdateServiceInstanceRequest request, ServiceInstance instance, String password) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        Map<String, Object> params = request.getParameters();
        if(params.get("user_name") != null && params.get("accesses") != null){
            // Assign user permissions/role to service instance
            String userName = (String) params.get("user_name");
            logger.info("Assign role, username:  " + userName + ", service instance id: " + serviceInstanceId );
            String accessesStr = (String) params.get("accesses");
            if (accessesStr != null && accessesStr.length() != 0){
                List<String> accesses = new ArrayList<>();
                // Convert accesses from string to list
                Collections.addAll(accesses, accessesStr.split(","));
                addUserToServiceInstance(ocdp, instance, userName, password, accesses);
            } else {
                logger.info("Skip add user to ServiceInstance if parameter 'accesses' is empty string.");
            }
        } else {
            // Resize service instance
            logger.info("Resizing service instance: " + serviceInstanceId);
            try{
                ocdp.resizeResourceQuota(instance, params);
            } catch (IOException e){
                e.printStackTrace();
                throw new OCDPServiceException(e.getMessage());
            }
            logger.info("Resizing service instance [{}] with params [{}] successful.", serviceInstanceId, params);
        }
        logger.info("Update service instance [{}] successfully!", serviceInstanceId);
        return new UpdateServiceInstanceResponse().withAsync(false);
    }

    private void addUserToServiceInstance(OCDPAdminService ocdp, ServiceInstance instance, String userName,
                                           String password, List<String> accesses) {
        // 1) Create LDAP user and krb principal for tenant user if it not exits
        if (createLDAPUser(userName)){
            createKrbPrinc(userName, password);
            etcdClient.write("/servicebroker/ocdp/user/krb/" + userName, password);
        }
        // 2) Create policy for service instance or append user to an exists policy
        String serviceInstancePolicyId = (String) instance.getServiceInstanceCredentials().get("rangerPolicyId");
        String serviceDefinitionId = instance.getServiceDefinitionId();
        String resourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        String serviceInstanceResource = (String) instance.getServiceInstanceCredentials().get(resourceType);
        if (serviceInstancePolicyId == null || serviceInstancePolicyId.length() == 0 ){
            // Create new ranger policy for service instance and update policy to service instance
            serviceInstancePolicyId = createPolicyForResources(ocdp, serviceInstanceResource, userName);
            updateServiceInstanceCredentials(instance, "rangerPolicyId", serviceInstancePolicyId);
        } else {
            // Append user to service instance policy
            updateUserToPolicy(ocdp, serviceInstancePolicyId, userName, accesses);
        }
    }

    public Map<String, Object> getOCDPServiceCredential(
            String serviceDefinitionId, String serviceInstanceId){
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        return ocdp.generateCredentialsInfo(serviceInstanceId);
    }

    private void updateServiceInstanceCredentials(ServiceInstance instance, String key, String value){
        Map<String, Object> credentials = instance.getServiceInstanceCredentials();
        credentials.replace(key, value);
        instance.setCredential(credentials);
        repository.save(instance);
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

    private boolean createLDAPUser(String userName) {
        boolean newCreatedLDAPUser = false;
        try{
            if(! BrokerUtil.isLDAPUserExist(this.ldap, userName)){
                logger.info("create new ldap user: " +  userName);
                newCreatedLDAPUser = true;
                BrokerUtil.createLDAPUser(this.ldap, this.etcdClient, userName,
                        clusterConfig.getLdapGroup(), clusterConfig.getLdapGroupId());
            }
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }
        return newCreatedLDAPUser;
    }

    private void createKrbPrinc(String userName, String password) {
        logger.info("create new kerberos principal.");
        String principalName = userName + "@" + clusterConfig.getKrbRealm();
        // Generate krb password and store it to etcd
        etcdClient.write("/servicebroker/ocdp/user/krb/" + principalName, password);
        try{
            kc.createPrincipal(principalName, password);
        }catch(KerberosOperationException e){
            logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            rollbackLDAPUser(userName);
            throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
        }
    }

    private String createTenentResource(OCDPAdminService ocdp, String serviceDefinitionId, String planId,
                                        String serviceInstanceId, Map<String, Object> params) {
        String serviceInstanceResource;
        try{
            serviceInstanceResource = ocdp.provisionResources(
                    serviceDefinitionId, planId, serviceInstanceId, params);
        }catch (Exception e){
            logger.error("OCDP resource provision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("OCDP ressource provision fails due to: " + e.getLocalizedMessage());
        }
        return serviceInstanceResource;
    }

    private void deleteTenentResource(OCDPAdminService ocdp, String serviceInstanceResource){
        try{
            ocdp.deprovisionResources(serviceInstanceResource);
        }catch (Exception e){
            logger.error("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
        }
    }

    private String createPolicyForResources(OCDPAdminService ocdp, String serviceInstanceResource, String userName){
        String policyId = null;
        int i = 0;
        logger.info("Try to create ranger policy...");
        while(i++ <= 40){
            policyId = ocdp.createPolicyForResources(serviceInstanceResource,
                    new ArrayList<String>(){{add(serviceInstanceResource);}}, userName, clusterConfig.getLdapGroup());
            // TODO Need get a way to force sync up ldap users with ranger service, for temp solution will wait 60 sec
            if (policyId == null){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else{
                logger.info("Ranger policy created. Policy ID = " + policyId);
                break;
            }
        }
        if (policyId == null){
            logger.error("Ranger policy create fail.");
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        return policyId;
    }

    private void updateUserToPolicy(
            OCDPAdminService ocdp, String serviceInstancePolicyId, String userName, List<String> accesses){
        int i = 0;
        boolean policyUpdateResult = false;
        logger.info("Try to append user to ranger policy...");
        while(i++ <= 40){
            policyUpdateResult = ocdp.appendUserToPolicy(
                    serviceInstancePolicyId, this.clusterConfig.getLdapGroup(), userName, accesses);
            if (!policyUpdateResult){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else{
                logger.info("Append user to ranger policy succeed. Policy ID = " + serviceInstancePolicyId);
                break;
            }
        }
        if (! policyUpdateResult){
            logger.error("Fail to append user [{}] to ranger policy [{}].", userName, serviceInstancePolicyId);
            throw new OCDPServiceException("Fail to append user to ranger policy.");
        }
    }

    private void rollbackLDAPUser(String userName) {
        logger.info("Rollback LDAP user: " + userName);
        try{
            BrokerUtil.removeLDAPUser(ldap, userName);
        }catch (Exception ex){
            logger.error("Delete LDAP user fail due to: " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }

    private void rollbackKrb(String principal) {
        logger.info("Rollback kerberos principal: " + principal);
        try{
            kc.removePrincipal(principal);
        }catch(KerberosOperationException ex){
            logger.error("Delete kerbreos principal fail due to: " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }

    private void rollbackResource(OCDPAdminService ocdp, String serviceInstanceResource) {
        logger.info("Rollback OCDP resource: " + serviceInstanceResource);
        try{
            ocdp.deprovisionResources(serviceInstanceResource);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}