package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import java.io.IOException;
import java.lang.Thread;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Future;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.OCDPUpdateServiceInstanceResponse;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
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
        String tenantName = request.getSpaceGuid();
        Map<String, Object> params = request.getParameters();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // 1) Create internal LDAP user for tenant if it not exist
        createLDAPUser(tenantName);

        // 2) Create big data resources like hdfs folder, hbase namespace ...
        String serviceInstanceResource = createTenentResource(
                ocdp, serviceDefinitionId, planId, serviceInstanceId, params);

        // 3) Create Ranger policy for tenant or append resource to exist tenant ranger policy.
        String tenantPolicyId = etcdClient.readToString(
                "/servicebroker/ocdp/tenants/" + tenantName + "/" + serviceDefinitionId);
        if(tenantPolicyId == null){
            tenantPolicyId = createPolicyForTenant(ocdp, serviceInstanceResource, tenantName);
            etcdClient.write("/servicebroker/ocdp/tenants/" + tenantName + "/" + serviceDefinitionId, tenantPolicyId);
        } else {
            if(! ocdp.appendResourceToTenantPolicy(tenantPolicyId, serviceInstanceResource)){
                logger.error("Fail to append resource [{}] to ranger policy [{}].", serviceInstanceResource, tenantPolicyId);
                throw new OCDPServiceException("Fail to append resource to ranger policy.");
            }
        }

        // 4) Generate service instance credential info
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(serviceInstanceId);
        String serviceResourceType = OCDPAdminServiceMapper.getOCDPResourceType(serviceDefinitionId);
        // For spark/mr instance provision, need append queue name into credentials,
        // because function generateCredentialsInfo not append it
        if(! credentials.containsKey(serviceResourceType))
            credentials.put(serviceResourceType, serviceInstanceResource);
        credentials.put("rangerPolicyId", tenantPolicyId);
        credentials.put("tenantName", tenantName);

        // 5) Save service instance
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
        String tenantName = (String)Credential.get("tenantName");
        String tenantPolicyId = (String)Credential.get("rangerPolicyId");

        // 1) Remove resource from ranger policy or delete policy
        List<String> policyResources = ocdp.getResourceFromTenantPolicy(tenantPolicyId);
        if(policyResources.size() == 1 && policyResources.get(0).equals(serviceInstanceResource)){
            boolean policyDeleteResult = ocdp.deletePolicyForTenant(tenantPolicyId);
            if(!policyDeleteResult)
            {
                logger.error("Ranger policy delete fail.");
                throw new OCDPServiceException("Ranger policy delete fail.");
            }
            etcdClient.delete("/servicebroker/ocdp/tenants/" + tenantName  + "/" + serviceDefinitionId);
        } else {
            ocdp.removeResourceFromTenantPolicy(tenantPolicyId, serviceInstanceResource);
        }

        // 2 )Delete big data resources like hdfs folder, hbase namespace ...
        deleteTenentResource(ocdp, serviceInstanceResource);

        // 3) Clean service instance from etcd
        repository.delete(serviceInstanceId);

		return new DeleteServiceInstanceResponse().withAsync(false);
	}

    @Async
    public Future<UpdateServiceInstanceResponse> doUpdateServiceInstanceAsync(
            UpdateServiceInstanceRequest request, String password) throws OCDPServiceException {
        return new AsyncResult<UpdateServiceInstanceResponse>(doUpdateServiceInstance(request, password));
    }

    public UpdateServiceInstanceResponse doUpdateServiceInstance(
            UpdateServiceInstanceRequest request, String password) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        Map<String, Object> params = request.getParameters();
        UpdateServiceInstanceResponse response;
        if(params.get("user_name") != null && params.get("tenant_name") != null){
            String accountName = (String) params.get("user_name");
            String tenantName = (String) params.get("tenant_name");
            if(params.get("accesses") != null) {
                // Assign role to tenant user
                Map<String, Object> accesses = (Map<String, Object>)params.get("accesses");
                addUserToTenant(ocdp, accountName, password, tenantName, accesses);
                Map<String, Object> credentials = new HashMap<String, Object>() {
                    {
                        put("username", accountName);
                        put("password", password);
                    }
                };
                response = new OCDPUpdateServiceInstanceResponse().withCredential(credentials).withAsync(false);
            } else {
                // Revoke role from tenant user
                removeUserFromTenant(ocdp, accountName, tenantName);
                response = new OCDPUpdateServiceInstanceResponse().withAsync(false);
            }
        } else {
            // Service instance resize
            String serviceInstanceId = request.getServiceInstanceId();
            ServiceInstance instance = repository.findOne(serviceInstanceId);
            if (instance == null) {
                throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
            }
            try{
                ocdp.resizeResourceQuota(instance, params);
            } catch (IOException e){
                e.printStackTrace();
            }
            response = new OCDPUpdateServiceInstanceResponse().withAsync(false);
        }
        return response;
    }

    private void addUserToTenant(OCDPAdminService ocdp, String accountName,
                                 String password, String tenantName, Map<String, Object> accesses) {
        //Create LDAP user and krb principal for tenant user if it not exits
        if (createLDAPUser(accountName)){
            createKrbPrinc(accountName, password);
        }
        //Assgin permissions/role to tenant user
        for(String serviceDefinitionId : accesses.keySet()){
            String tenantPolicyId = etcdClient.readToString(
                    "/servicebroker/ocdp/tenants/" + tenantName + "/" + serviceDefinitionId);
            List<String> permissions = (List<String>)accesses.get(serviceDefinitionId);
            int i = 0;
            boolean policyUpdateResult = false;
            logger.info("Try to update ranger policy...");
            while(i++ <= 40){
                policyUpdateResult = ocdp.appendUserToTenantPolicy(
                        tenantPolicyId, this.clusterConfig.getLdapGroup(), accountName, permissions);
                if (!policyUpdateResult){
                    try{
                        Thread.sleep(3000);
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }else{
                    logger.info("Ranger policy updated.");
                    break;
                }
            }
            if (! policyUpdateResult){
                rollbackLDAPUser(accountName);
                rollbackKrb(accountName + "@" + clusterConfig.getKrbRealm());
                throw new OCDPServiceException("Ranger policy update fail.");
            }
        }
    }

    private void removeUserFromTenant(OCDPAdminService ocdp, String accountName, String tenantName) {
        //Unassign permissions/role from tenant user
        for(String id : OCDPAdminServiceMapper.getOCDPServiceIds()){
            String tenantPolicyId = etcdClient.readToString(
                    "/servicebroker/ocdp/tenants/" + tenantName + "/" + id);
            if(id != null){
                ocdp.removeUserFromTenantPolicy(tenantPolicyId, accountName);
            }
        }
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

    private String createPolicyForTenant(OCDPAdminService ocdp, String serviceInstanceResource, String tenantName){
        String policyId = null;
        int i = 0;
        logger.info("Try to create ranger policy...");
        while(i++ <= 40){
            policyId = ocdp.createPolicyForTenant(tenantName, new ArrayList<String>(){{add(serviceInstanceResource);}},
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
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        return policyId;
    }

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