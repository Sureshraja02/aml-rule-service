package com.aml.srv.core.efrmsrv.cust.profiling.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.kafka.repo.FinSecIndicatorVO;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class CustomerProfiling {

	private static final Logger Logger = LoggerFactory.getLogger(CustomerProfiling.class);
	
	public FinSecIndicatorVO addCustomerProfilingStsFinSecIndictor(FinSecIndicatorVO finSecIndicatorVOObjParam){
		
		// Need to do customer profiling
		
		
		//finSecIndicatorVOObjParam.setCustomerProfileIsReady(AMLConstants.YES);
		return finSecIndicatorVOObjParam;
	}

}
