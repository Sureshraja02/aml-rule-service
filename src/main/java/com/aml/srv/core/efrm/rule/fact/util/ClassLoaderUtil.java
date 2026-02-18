package com.aml.srv.core.efrm.rule.fact.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.rule.service.RulesIdentifierService;

@Service
public class ClassLoaderUtil {

	private Logger LOGGER = LoggerFactory.getLogger(RulesIdentifierService.class);

	@Autowired
	private ApplicationContext context;

	public <T> T getBean(String beanName, Class<T> clazz) {
		if (context.containsBean(beanName))
			return context.getBean(beanName, clazz);
		else
			return context.getBean(beanName, clazz);

	}

}
