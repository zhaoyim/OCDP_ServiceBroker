package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.*;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.http.auth.AuthScope;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java Client for manipulate Apache Ranger.
 *
 * @author whitebai1986@gmail.com
 *
 */
public class rangerClient {

    private CloseableHttpClient httpClient;
    private HttpClientContext context;
    private URI baseUri;
    private Logger logger = LoggerFactory.getLogger(rangerClient.class);

    static final Gson gson = new GsonBuilder().create();

    public rangerClient(String uri, String username, String password){

        if(! uri.endsWith("/")){
            uri += "/";
        }
        this.baseUri = URI.create(uri);

        this.httpClient = HttpClientBuilder.create().build();

        HttpHost targetHost = new HttpHost(this.baseUri.getHost(), 6080, "http");
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM),
                new UsernamePasswordCredentials(username, password));
        AuthCache authCache = new BasicAuthCache();
        authCache.put(targetHost, new BasicScheme());
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(provider);
        context.setAuthCache(authCache);
        this.context = context;
    }

    public String getV2Policy(String policyID){
        return doGetPolicy("service/public/v2/api/policy", policyID);
    }

    private String doGetPolicy(String url, String policyID){
        String policyDef = null;
        URI uri = buildPolicyUri(url, policyID, "");
        HttpGet request = new HttpGet(uri);
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200){
                policyDef = EntityUtils.toString(response.getEntity());
            }
            response.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return policyDef;
    }

    public String createV2Policy(RangerV2Policy policy) {
        return doCreateV2Policy("service/public/v2/api/policy", policy);
    }

    private String doCreateV2Policy(String url, RangerV2Policy policy){
        String newPolicyString = null;
        String policyDef = gson.toJson(policy);
        logger.info("DEBUG info:" + policyDef);
        URI uri = buildPolicyUri(url, "", "");
        HttpPost request = new HttpPost(uri);
        StringEntity entity = new StringEntity(policyDef, HTTP.UTF_8);
        entity.setContentType("application/json");
        request.setEntity(entity);
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200)
            {
                newPolicyString = EntityUtils.toString(response.getEntity(),"UTF-8");
            }else{
                logger.error("Ranger policy create fail due to: " + response.getStatusLine());
            }
            response.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return newPolicyString;
    }

    public boolean removeV2Policy(String policyID){
        return doRemovePolicy("service/public/v2/api/policy", policyID);
    }

    private boolean doRemovePolicy(String url, String policyID){
        boolean status = false;
        URI uri = buildPolicyUri(url, policyID, "");
        HttpDelete request = new HttpDelete(uri);
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            status = (response.getStatusLine().getStatusCode() == 204);
            response.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return status;
    }

    public boolean updateV2Policy(String policyID, RangerV2Policy policy){
        return doUpdatePolicy("service/public/v2/api/policy/", policyID, policy);
    }

    private boolean doUpdatePolicy(String url, String policyID, RangerV2Policy policy){
        boolean status = false;
        String policyUpdateDef = gson.toJson(policy);
        URI uri = buildPolicyUri(url + policyID, "", "");
        HttpPut request = new HttpPut(uri);
        StringEntity entity = new StringEntity(policyUpdateDef, HTTP.UTF_8);
        entity.setContentType("application/json");
        request.setEntity(entity);
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            status = (response.getStatusLine().getStatusCode() == 200);
            if (!status) {
                logger.error("Update policy [{}] failed: " + response.getStatusLine().getReasonPhrase(), policyID);
			}
            response.close();
            logger.info("Update ranger policy successfully!");
        }catch (IOException e){
            e.printStackTrace();
        }
        return status;
    }

    public  List<String> getResourcsFromV2Policy(String policyId, String resourcetype){
        String currentPolicy = getV2Policy(policyId);
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        return rp.getResourceValues(resourcetype);
    }

    public List<String> getUsersFromV2Policy(String policyId) {
        String currentPolicy = getV2Policy(policyId);
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        return rp.getUserList();
    }

    public boolean appendResourceToV2Policy(String policyId, String serviceInstanceResource, String resourceType) {
        String currentPolicy = getV2Policy(policyId);
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.updateResource(resourceType, serviceInstanceResource);
        return updateV2Policy(policyId, rp);
    }

    public boolean removeResourceFromV2Policy(String policyId, String serviceInstanceResource, String resourceType){
        String currentPolicy = getV2Policy(policyId);
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.removeResource(resourceType, serviceInstanceResource);
        return updateV2Policy(policyId, rp);
    }

    public boolean appendUserToV2Policy(String policyId, String groupName, String userName, List<String> permissions) {
        String currentPolicy = getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        if (rp.getUserList().contains(userName)){
            // Refresh accesses list if user already exist in policy
            rp.updateUserAccesses(userName, permissions);
        } else {
            // Append new policyItem if user not exist in policy
            rp.addPolicyItems(new ArrayList<String>(){{add(userName);}},
                    new ArrayList<String>(){{add(groupName);}}, new ArrayList<>(), true, permissions);
        }
        return updateV2Policy(policyId, rp);
    }

    public boolean removeUserFromV2Policy(String policyId, String userName){
        String currentPolicy = getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.removePolicyItem(userName);
        return updateV2Policy(policyId, rp);
    }

    private URI buildPolicyUri(String prefix, String key, String suffix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        for (String token : Splitter.on('/').split(key)) {
            sb.append("/");
            sb.append(urlEscape(token));
        }
        sb.append(suffix);

        URI uri = this.baseUri.resolve(sb.toString());
        return uri;
    }

    protected static String urlEscape(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException();
        }
    }
}
