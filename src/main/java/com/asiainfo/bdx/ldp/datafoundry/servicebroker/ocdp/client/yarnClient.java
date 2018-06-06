package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.http.HttpHost;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

/**
 * Created by Aaron on 16/7/26.
 */
public class yarnClient{

    private CloseableHttpClient httpClient;
    private List<HttpClientContext> contexts;
    private List<URI> baseUris;

    private String totalMemory;
    private String availableMemory;
    private String allocateMemory;
    private Logger logger = LoggerFactory.getLogger(yarnClient.class);

    private void buildContext(String uri) {
        if(! uri.endsWith("/")){
            uri += "/";
        }
        URI baseUri = URI.create(uri);
        this.baseUris.add(baseUri);
        this.httpClient = HttpClientBuilder.create().build();
        HttpHost targetHost = new HttpHost(baseUri.getHost(), baseUri.getPort(), baseUri.getScheme());
        AuthCache authCache = new BasicAuthCache();
        authCache.put(targetHost, new BasicScheme());
        HttpClientContext context = HttpClientContext.create();
        context.setAuthCache(authCache);
        this.contexts.add(context);
    }

    public yarnClient(String uri) {
        this.contexts =new ArrayList<>();
        this.baseUris =new ArrayList<>();
        buildContext(uri);
    }

    public yarnClient(String uri1, String uri2) {
        this.contexts =new ArrayList<>();
        this.baseUris =new ArrayList<>();
        buildContext(uri1);
        buildContext(uri2);

    }

    public void getClusterMetrics()  {
        URI uri = buildUri("","","ws/v1/cluster/metrics",this.baseUris.get(0));
        URI uri2 = buildUri("","","ws/v1/cluster/metrics",this.baseUris.get(1));

        List<HttpGet> requests = new ArrayList<>();
        requests.add(new HttpGet(uri));
        requests.add(new HttpGet(uri2));


        String jsonStr = executeRequest(requests);

        this.allocateMemory = getMetrics("allocatedMB",jsonStr);

        this.availableMemory = getMetrics("availableMB",jsonStr);

        this.totalMemory = String.valueOf(Double.parseDouble(this.allocateMemory) + Double.parseDouble(this.availableMemory));
    }

    private String getMetrics(String key, String jsonStr){
        String value = null;

        try{
            Map<?,?> response;
            Gson gson = new Gson();
            java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<Map<?, ?>>() {
            }.getType();
            response = gson.fromJson(jsonStr, type);

            LinkedTreeMap<?,?> clusterMetrics = (LinkedTreeMap<?, ?>) response.get("clusterMetrics");
            value = String.valueOf(clusterMetrics.get(key));
        }catch (Exception e){
            logger.error("getMetrics() hit Exception: " , e);
        }
        return value;
    }

    public String getTotalMemory(){ return this.totalMemory;}
    public String getAvailableMemory(){ return  this.availableMemory;}
    public String getAllocateMemory(){ return this.allocateMemory;}

    private String executeRequest(List<HttpGet> requests) {
        String responseDef = null;
        for (int i=0; i<requests.size(); i++) {
            try {
                CloseableHttpResponse response = this.httpClient.execute(requests.get(i), this.contexts.get(i));
                if (response.getStatusLine().getStatusCode() == 200) {
                    responseDef = EntityUtils.toString(response.getEntity());
                }
                response.close();
            } catch (IOException e) {
//                e.printStackTrace();
                logger.warn(e.getMessage());
            }
            if (responseDef != null) return responseDef;
        }
//        throw new IOException("Connection to Yarn Resource Manager failed!");
        return null;
    }

    private URI buildUri(String prefix, String key, String suffix, URI baseUri) {
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

        URI uri = baseUri.resolve(sb.toString());
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
