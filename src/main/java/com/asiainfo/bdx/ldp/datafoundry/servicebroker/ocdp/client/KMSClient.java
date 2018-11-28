package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.collection.mutable.StringBuilder;

/**
 * KMS service client
 *
 * @author EthanWang 2018-11-27
 *
 */
@Component
public class KMSClient {
	private static final Logger LOG = LoggerFactory.getLogger(KMSClient.class);
	private final static String CONTENT_TYPE = "Content-type";
	private static final String CONTENT_TYPE_VALUE = "application/json;charset=utf-8";
	private static final String APPLICATION_JSON = "application/json";
	private static final int RSP_OK = 200;
	private static final String UTF_8 = "utf-8";
	// refering to connecting string like http://ip:port
	private final String baseURI;
	// config file
	private ClusterConfig config;

	/**
	 * Apply for keytab of specified user
	 * 
	 * @return principal and download link of corresponding keytab
	 */
	public Pair<String, String> applyKeytab(String username) {
		String url = baseURI + "/v1/api/kerberos/keytab/" + username;
		CloseableHttpClient httpclient = HttpClients.custom().build();
		try {
			HttpPost httpPost = getHTttpPost(url);
			setHttpEntity(httpPost, getHttpEntity(username));
			CloseableHttpResponse response = httpclient.execute(httpPost);
			if (suceed(response)) {
				return getPrincipalAndUrl(response);
			}
			LOG.error("Creating keytab failed from KMS: " + response);
			throw new RuntimeException(
					"Creating keytab failed from KMS: " + response.getStatusLine().getReasonPhrase());
		} catch (Exception e) {
			LOG.error("Exception while Creating keytab: " + username, e);
			throw new RuntimeException("Exception while Creating keytab: " + username, e);
		} finally {
			forceClose(httpclient);
		}
	}

	/**
	 * Close the http client
	 * 
	 * @param httpclient
	 */
	private void forceClose(CloseableHttpClient httpclient) {
		try {
			if (httpclient != null) {
				httpclient.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean suceed(CloseableHttpResponse response) {
		return response.getStatusLine().getStatusCode() == RSP_OK;
	}

	private void setHttpEntity(HttpEntityEnclosingRequestBase httpRequest, StringEntity httpEntity) {
		httpRequest.setEntity(httpEntity);
	}

	private HttpPost getHTttpPost(String url) {
		HttpPost httpPost = new HttpPost(url);
		httpPost.addHeader(CONTENT_TYPE, CONTENT_TYPE_VALUE);
		return httpPost;
	}

	private Pair<String, String> getPrincipalAndUrl(CloseableHttpResponse response) throws ParseException, IOException {
		String rspEntity = EntityUtils.toString(response.getEntity(), Charset.forName(UTF_8));
		JsonParser parser = new JsonParser();
		JsonElement json = parser.parse(rspEntity);
		String principal = json.getAsJsonObject().getAsJsonPrimitive(KMSRequestTemplate.CreatKeytab.KEY_PRINCIPAL)
				.getAsString();
		String kid = json.getAsJsonObject().getAsJsonPrimitive(KMSRequestTemplate.CreatKeytab.KEY_URL).getAsString();
		return new Pair<String, String>(principal, kid);
	}

	private StringEntity getHttpEntity(String username) throws UnsupportedEncodingException {
		StringEntity se = new StringEntity(KMSRequestTemplate.CreatKeytab.getEntity(config));
		se.setContentType(APPLICATION_JSON);
		se.setContentEncoding(UTF_8);
		return se;
	}

	@Autowired
	public KMSClient(ClusterConfig config) {
		this.config = config;
		baseURI = getBaseURI();
	}

	private String getBaseURI() {
		StringBuilder sb = new StringBuilder();
		sb.append("http://").append(config.getKms_ip()).append(":").append(config.getKms_port()).append("/");
		return sb.toString();
	}

	/**
	 * KMS request template collection
	 *
	 * @author EthanWang 2018-11-28
	 *
	 */
	private static class KMSRequestTemplate {
		/**
		 * Request template for creating keytab
		 *
		 * @author EthanWang 2018-11-28
		 *
		 */
		private static class CreatKeytab {
			private static final String KEY_ADMIN = "op_name";
			private static final String KEY_PASSWD = "password";
			private static final String KEY_URL = "url";
			private static final String KEY_PRINCIPAL = "principalName";

			/**
			 * return entity body of request of creating keytab
			 * 
			 * @return
			 */
			public static String getEntity(ClusterConfig config) {
				JsonObject obj = new JsonObject();
				obj.addProperty(KEY_ADMIN, config.getKms_admin_name());
				obj.addProperty(KEY_PASSWD, config.getKms_admin_password());
				return obj.toString();
			}
		}
	}

}
