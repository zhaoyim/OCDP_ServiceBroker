package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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
	private static final String FILE;
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

	static {
		try {
			String base = ServiceCatalogTemplate.class.getResource("/").getPath();
			FILE = base + File.separator + "service-catalog-template.json";
		} catch (Exception e) {
			LOG.error("Exception while init class: ", e);
			throw new RuntimeException("Exception while init class: ", e);
		}
	}

	@Autowired
	public ServiceCatalogTemplate(ClusterConfig clusterConfig) {
		String jsonString = parseTemplate(FILE);
		jsonString = jsonString.replaceAll(CLUSTER, clusterConfig.getClusterName());
		template = new JsonParser().parse(jsonString).getAsJsonObject();
	}

	private static String parseTemplate(String filename) {
		BufferedReader br = null;
		FileReader reader = null;
		try {
			File file = new File(filename);
			if (!file.exists()) {
				LOG.error("Template file not exist: " + file.getPath());
				throw new RuntimeException("Template file not exist: " + file.getPath());
			}
			reader = new FileReader(file);
			br = new BufferedReader(reader);
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			return sb.toString();
		} catch (Exception e) {
			LOG.error("Exception while parsing file: " + filename, e);
			throw new RuntimeException("Exception while parsing file: " + filename, e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					LOG.error("Exception while close FileReader: ", e);
				}
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					LOG.error("Exception while close FileReader: ", e);
				}
			}
		}
	}
}
