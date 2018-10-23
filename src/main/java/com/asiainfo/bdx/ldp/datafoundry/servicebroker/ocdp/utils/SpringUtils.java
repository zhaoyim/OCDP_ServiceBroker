package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import org.springframework.beans.BeansException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component(value = "springUtils")
public class SpringUtils implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		setContext(applicationContext);
	}
	
	private static void setContext(ApplicationContext applicationContext) {
		SpringUtils.applicationContext = applicationContext;
	}
	
	public static ApplicationContext getContext() {
	    return applicationContext;
	}
}
