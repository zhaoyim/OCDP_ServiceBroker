package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Aaron on 16/7/20.
 */
public class ambariClient {

    private Logger logger = LoggerFactory.getLogger(ambariClient.class);
    private CloseableHttpClient httpClient;
    private HttpClientContext context;
    private URI baseUri;
    private String clusterName;
    //@Autowired
    //private ApplicationContext appContext;

    static final Gson gson = new GsonBuilder().create();

    public ambariClient(String uri, String username, String password, String clusterName){

        if(! uri.endsWith("/")){
            uri += "/";
        }
        this.baseUri = URI.create(uri);

        this.httpClient = HttpClientBuilder.create().build();

        HttpHost targetHost = new HttpHost(this.baseUri.getHost(), 8080, "http");
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM),
                new UsernamePasswordCredentials(username, password));
        AuthCache authCache = new BasicAuthCache();
        authCache.put(targetHost, new BasicScheme());
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(provider);
        context.setAuthCache(authCache);
        this.context = context;
        this.clusterName = clusterName;
        //this.config = (ClusterConfig)this.appContext.getBean("clusterConfig");
    }

    private String getCapacitySchedulerTag(String rmHost){
        URI uri = buildUri("api/v1/clusters/" + this.clusterName + "/hosts",rmHost,
                "/host_components/RESOURCEMANAGER?fields=HostRoles/actual_configs/capacity-scheduler");

        HttpGet request = new HttpGet(uri);

        String jsonStr = executeRequest(request);

        return getVersionTagFromJson(jsonStr);
    }

    public String getCapacitySchedulerConfig(String rmHost){

        String versionTag = getCapacitySchedulerTag(rmHost);

        URI uri = buildUri("api/v1/clusters/" + this.clusterName,"","configurations?type=capacity-scheduler&tag="+versionTag);

        HttpGet request = new HttpGet(uri);

        return executeRequest(request);

    }

    /**
     * PUT Capacity-Scheduler Config to Ambari
     * @param properties
     * @return  desired config
     */
    public String updateCapacitySchedulerConfig(Map<String,String> properties, String clusterName){

        Date now = new Date();
        String updateStr = null;
        String csProperties = gson.toJson(properties);

        updateStr = "{\"Clusters\": {\"desired_config\": {\"type\":\"capacity-scheduler\",\"tag\":\"version"
                + now.getTime() +"\",\"properties\":";
        updateStr += csProperties+"}}}";

        URI uri = buildUri("api/v1/clusters","",clusterName);

        HttpPut request = new HttpPut(uri);

        StringEntity entity = new StringEntity(updateStr, HTTP.UTF_8);

        request.setEntity(entity);
        request.setHeader("X-Requested-By","ambari");

        return executeRequest(request);

    }

    /**
     * Send refresh queue request to ambari
     * @return accept or error
     */
    public String refreshYarnQueue(String rmHost){

        String refreshRequestEntity = buildRequestEntity("YARN",rmHost,"RESOURCEMANAGER",
                "capacity-scheduler","Refresh YARN Capacity Scheduler","REFRESHQUEUES");

        URI uri = buildUri("api/v1/clusters/" + this.clusterName + "/requests/","","");

        HttpPost request = new HttpPost(uri);

        StringEntity entity = new StringEntity(refreshRequestEntity, HTTP.UTF_8);

        request.setEntity(entity);
        request.setHeader("X-Requested-By","ambari");

        return executeRequest(request);

//        return null;
    }

    /**
     * Send restart yarn resource manager request to ambari
     * @return
     */

    public String restartResourceManager(){

        return null;

    }

    /**
     * build refresh request
     * @param serviceName
     * @param hosts
     * @param componentName
     * @param configTags
     * @param context
     * @param command
     * @return
     */
    private String buildRequestEntity(String serviceName,String hosts,String componentName,
                                      String configTags,String context,String command){
        String requestEntity = null;

        requestEntity = "{\"Requests\\/resource_filters\":" +
                "[{\"service_name\":"+"\""+serviceName+"\""+
                ",\"hosts\":"+"\""+hosts+"\""+
                ",\"component_name\":"+"\""+componentName+"\""+
                "}]," +
                "\"RequestInfo\":" +
                "{\"parameters\\/forceRefreshConfigTags\":"+"\""+configTags+"\""+
                ",\"context\":"+"\""+context+"\""+
                ",\"command\":"+"\""+command+"\""+
                "}}";
//        requestEntity += serviceName;
        return requestEntity;

    }

    /**
     * build restart request
     * @param serviceName
     * @param hosts
     * @param componentName
     * @param level
     * @param clusterName
     * @param context
     * @param command
     * @return
     */
    private String buildRequestEntity(String serviceName,String hosts,String componentName,String level,
                                      String clusterName,String context,String command){
        return null;
    }

    /**
     *
     * @param jsonStr
     * @return versionTag
     */
    private String getVersionTagFromJson(String jsonStr)
    {
    	Preconditions.checkNotNull(jsonStr, "Response is null from RM restAPI.");
        String finalStr = null;
        try {
            Map<?, ?> response = null;
            Gson gson = new Gson();
            java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?, ?>>() {
            }.getType();
            response = gson.fromJson(jsonStr, type);

            LinkedTreeMap<?,?> firstLevel = (LinkedTreeMap<?, ?>) response.get("HostRoles");
            LinkedTreeMap<?,?> secondLevel = (LinkedTreeMap<?, ?>) firstLevel.get("actual_configs");
            LinkedTreeMap<?,?> thirdLevel = (LinkedTreeMap<?, ?>) secondLevel.get("capacity-scheduler");

            finalStr = (String) thirdLevel.get("default");

        }catch (Exception e){
            e.printStackTrace();
        }
        return finalStr;

    }

    private String executeRequest(HttpUriRequest request)
    {
        String responseDef = null;
        try{
            CloseableHttpResponse response = this.httpClient.execute(request, this.context);
            if(response.getStatusLine().getStatusCode() == 200){
                responseDef = EntityUtils.toString(response.getEntity());
            }
            response.close();
        }catch (IOException e){
        	logger.error("Error while execute HTTP request: " + request.getURI(), e);
            e.printStackTrace();
        }
        return responseDef;
    }

    private URI buildUri(String prefix, String key, String suffix) {
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
