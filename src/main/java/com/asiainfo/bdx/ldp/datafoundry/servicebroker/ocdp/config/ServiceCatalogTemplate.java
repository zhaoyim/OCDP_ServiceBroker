package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Catalog template
 *
 * @author EthanWang 2018-10-11
 *
 */
@Component
public class ServiceCatalogTemplate {
	private static final Logger LOG = LoggerFactory.getLogger(ServiceCatalogTemplate.class);
	private static final String CLUSTER = Pattern.quote("${clustername}");
	private JsonObject template;

	public List<ServiceDefinition> getServices() {
		Preconditions.checkNotNull(template, "Template is null");
		List<ServiceDefinition> serviceDefinitions = Lists.newArrayList();
		JsonArray services = template.getAsJsonArray("serviceDefinitions");
		services.forEach(s -> {
			serviceDefinitions.add(toServiceDefinition(s));
		});
		return serviceDefinitions;
	}

	private ServiceDefinition toServiceDefinition(JsonElement s) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			ServiceDefinition svcdef = mapper.readValue(s.toString(), ServiceDefinition.class);
			return svcdef;
		} catch (IOException e) {
			LOG.error("Exception while parsing catalog service: " + s.toString(), e);
			throw new RuntimeException("Exception while parsing catalog service: " + s.toString(), e);
		}
	}

	@Autowired
	public ServiceCatalogTemplate(ClusterConfig clusterConfig) {
		String jsonString = parseTemplate("service-catalog-template.json");
		jsonString = jsonString.replaceAll(CLUSTER, clusterConfig.getClusterName());
		template = new JsonParser().parse(jsonString).getAsJsonObject();
	}

	private static String parseTemplate(String filename) {
		InputStream in = null;
		try {
			in = ServiceCatalogTemplate.class.getClassLoader().getResourceAsStream(filename);
			String content = IOUtils.toString(in, Charset.forName("utf-8"));
			if (Strings.isNullOrEmpty(content)) {
				LOG.error("Content is null: " + filename);
				throw new RuntimeException("Content is null: " + filename);
			}
			return content;
		} catch (Exception e) {
			LOG.error("Exception while parsing file: " + filename, e);
			throw new RuntimeException("Exception while parsing file: " + filename, e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					LOG.error("Exception while close InputStream: ", e);
				}
			}
		}
	}
}
