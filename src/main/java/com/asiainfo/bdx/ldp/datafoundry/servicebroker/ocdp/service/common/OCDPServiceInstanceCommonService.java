package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import java.lang.Thread;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OCDPCreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
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
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;

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

    @Autowired
    public OCDPServiceInstanceCommonService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.kc = new krbClient(clusterConfig);
        this.etcdClient = clusterConfig.getEtcdClient();
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

        // Create big data resources like hdfs folder, hbase namespace ...
        String serviceInstanceResource = createTenentResource(
                ocdp, serviceDefinitionId, planId, serviceInstanceId, params);

        // Generate service instance credential info
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(serviceInstanceId);
        String serviceResourceType = ocdp.getServiceResourceType();
        // For spark/mr instance provision, need append queue name into credentials,
        // because function generateCredentialsInfo not append it
        if(! credentials.containsKey(serviceResourceType))
            credentials.put(serviceResourceType, serviceInstanceResource);

        // Save service instance
        ServiceInstance instance = new ServiceInstance(request);
        instance.setCredential(credentials);
        repository.save(instance);

        CreateServiceInstanceResponse response = new OCDPCreateServiceInstanceResponse()
                .withCredential(credentials)
                .withAsync(false);
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
        String serviceResourceType = ocdp.getServiceResourceType();
        String serviceInstanceResource = (String)Credential.get(serviceResourceType);

        // Delete big data resources like hdfs folder, hbase namespace ...
        deleteTenentResource(ocdp, serviceInstanceResource);

        // Clean cache from etcd
        repository.delete(serviceInstanceId);

		return new DeleteServiceInstanceResponse().withAsync(false);
	}

    public Map<String, Object> getOCDPServiceCredential(
            String serviceDefinitionId, String serviceInstanceId){
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        return ocdp.generateCredentialsInfo(serviceInstanceId);
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

    private boolean createLDAPUser(String accountName) {
        boolean newCreatedLDAPUser = false;
        try{
            if(! BrokerUtil.isLDAPUserExist(this.ldap, accountName)){
                logger.info("create new ldap user.");
                newCreatedLDAPUser = true;
                BrokerUtil.createLDAPUser(this.ldap, this.etcdClient, accountName,
                        clusterConfig.getLdapGroup(), clusterConfig.getLdapGroupId());
            }
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }
        return newCreatedLDAPUser;
    }

    private void createKrbPrinc(String accountName, String password) {
        logger.info("create new kerberos principal.");
        String principalName = accountName + "@" + clusterConfig.getKrbRealm();
        // Generate krb password and store it to etcd
        etcdClient.write("/servicebroker/ocdp/user/krb/" + principalName, password);
        try{
            kc.createPrincipal(principalName, password);
        }catch(KerberosOperationException e){
            logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            rollbackLDAPUser(accountName);
            throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
        }
    }

    private String createTenentResource(OCDPAdminService ocdp, String serviceDefinitionId, String planId,
                                        String serviceInstanceId, Map<String, Object> params) {
        String serviceInstanceResource;
        try{
            serviceInstanceResource = ocdp.provisionResources(
                    serviceDefinitionId, planId, serviceInstanceId, null, params);
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

    private String createPolicyForTenant(OCDPAdminService ocdp, String serviceInstanceResource,
                                         String tenantName, boolean newCreatedLDAPUser){
        String policyName = UUID.randomUUID().toString();
        String policyId = null;
        int i = 0;
        logger.info("Try to create ranger policy...");
        while(i++ <= 40){
            policyId = ocdp.assignPermissionToResources(policyName, new ArrayList<String>(){{add(serviceInstanceResource);}},
                    tenantName, clusterConfig.getLdapGroup());
            // TODO Need get a way to force sync up ldap users with ranger service, for temp solution will wait 60 sec
            if (policyId == null){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else{
                logger.info("Ranger policy created.");
                break;
            }
        }
        if (policyId == null){
            logger.error("Ranger policy create fail.");
            if(newCreatedLDAPUser){
                rollbackLDAPUser(tenantName);
                rollbackKrb(tenantName + "@" + clusterConfig.getKrbRealm());
            }
            rollbackResource(ocdp, serviceInstanceResource);
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        return policyId;
    }



    private String grantPrivilegeForUser(OCDPAdminService ocdp, String tenantName, String accountName,
                                         List<String> access) {
        return "";
    }

    private String revokePrivilegeForUser(OCDPAdminService ocdp, String tenantName, String accountName) {
        return "";
    }

    private void resizeResourceQuota() {}

    private void rollbackLDAPUser(String accountName) {
        logger.info("Rollback LDAP user: " + accountName);
        try{
            BrokerUtil.removeLDAPUser(ldap, accountName);
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