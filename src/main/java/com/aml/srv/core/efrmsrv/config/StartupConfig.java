package com.aml.srv.core.efrmsrv.config;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.aml.srv.core.efrmsrv.utils.CommonUtils;

@Configuration
public class StartupConfig {

	private static final Logger Logger = LoggerFactory.getLogger(StartupConfig.class);
	
	public static ConcurrentHashMap<String, String> amlTableNameMap = new ConcurrentHashMap<String, String>();

	@Autowired
	CommonUtils commonUtils;

	@Bean
	void toGetTableName() {
		commonUtils.toLoadMapFromPro();
		Logger.info("AML Table Map Loaded...");
	}
}
