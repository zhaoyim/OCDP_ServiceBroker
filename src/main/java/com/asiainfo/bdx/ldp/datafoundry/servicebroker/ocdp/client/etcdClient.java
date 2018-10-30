package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client;

import java.net.URI;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdResult;

/**
 * ETCD client for DP-Brokers. Opertions of both read/write to ETCD by this client
 * would have prefix of <code>/dp-brokers/broker-id</code>  appended to the head of 
 * path(the prefix is supposed to support for sharing ETCD storage of multiple brokers).
 *
 * @author Ethan
 *
 */
public class etcdClient {

	private EtcdClient innerClient;
	public final String PATH_PREFIX;

	public etcdClient(String etcd_host, String etcd_port, String etcd_user, String etcd_password, String brokerId) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(brokerId), "BrokerID must not be null");
		this.innerClient = new EtcdClient(
				URI.create("http://" + etcd_user + ":" + etcd_password + "@" + etcd_host + ":" + etcd_port));
		PATH_PREFIX = "/dp-brokers/" + brokerId;
	}
	
	private String assemblePath(String suffix) {
		if (!suffix.startsWith("/")) {
			System.out.println("ERROR: Path not start with '/': " + suffix);
			throw new RuntimeException("Path must start with '/': " + suffix);
		}
		return this.PATH_PREFIX + suffix;
	}

	public EtcdResult read(String key) {
		EtcdResult result = new EtcdResult();
		try {
			result = this.innerClient.get(assemblePath(key));
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		return result;
	}

	public String readToString(String key) {
		EtcdResult result = this.read(key);
		return (result != null && result.node != null) ? result.node.value : null;
	}

	public EtcdResult write(String key, String value) {
		EtcdResult result = new EtcdResult();
		try {
			result = this.innerClient.set(assemblePath(key), value);
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		return result;
	}

	public EtcdResult createDir(String key) {
		EtcdResult result = new EtcdResult();
		try {
			result = this.innerClient.createDirectory(assemblePath(key));
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		return result;
	}

	public EtcdResult delete(String key) {
		EtcdResult result = new EtcdResult();
		try {
			result = this.innerClient.delete(assemblePath(key));
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		return result;
	}

	public EtcdResult deleteDir(String key, boolean recursive) {
		EtcdResult result = new EtcdResult();
		try {
			result = this.innerClient.deleteDirectory(assemblePath(key), recursive);
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		return result;
	}

}
