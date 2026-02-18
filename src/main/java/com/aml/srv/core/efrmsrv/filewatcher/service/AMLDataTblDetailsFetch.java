
package com.aml.srv.core.efrmsrv.filewatcher.service;

import java.util.UUID;

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
public class AMLDataTblDetailsFetch {

	private static final Logger Logger = LoggerFactory.getLogger(AMLDataTblDetailsFetch.class);

	/***
	 * 
	 * @return toSetFinSecIndicatorObjectForDuckDBSts FinSecIndicatorVO
	 */
	public FinSecIndicatorVO toSetFinSecIndicatorObjectForDuckDBSts(FinSecIndicatorVO finSecIndicatorVOObj) {
		//FinSecIndicatorVO finSecIndicatorVOObj = null;
		try {
			String uuid = UUID.randomUUID().toString();

			
			finSecIndicatorVOObj.setUuid(uuid);
			finSecIndicatorVOObj.setCsv2DuckDbImprtIsReady(AMLConstants.YES);
			// finSecIndicatorVOObj.setCustomerProfileIsReady(AMLConstants.YES);

			finSecIndicatorVOObj.setAccCustTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setAccountTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setAsaTblIsReady(AMLConstants.YES); // Account Status Table
			finSecIndicatorVOObj.setBranchTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setChequeDtlTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setCountryTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setCrmTblIsReady(AMLConstants.YES); // Currency Mgmt Table
			finSecIndicatorVOObj.setCstTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setEodTblIsReady(AMLConstants.YES);// EOD Table
			finSecIndicatorVOObj.setInsturmentTblIsReady(AMLConstants.YES);// Instrument Table
			finSecIndicatorVOObj.setJoinHolderTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setLockerTblsIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setMcdTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setMinAccBalanceTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setNcbTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setNctTransTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setNomineeTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setProductTblIsReady(AMLConstants.YES);// Product Table
			finSecIndicatorVOObj.setTradeAccTransTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setTransactionTblIsReady(AMLConstants.YES);
			finSecIndicatorVOObj.setTransTypeTblIsReady(AMLConstants.YES);

		} catch (Exception e) {
			Logger.error("Exception found in FileWatcher@toSetFinSecIndicatorObject : {}", e);
		} finally {

		}
		return finSecIndicatorVOObj;
	}

	public FinSecIndicatorVO toGetRowCountEachAMLTblSetINFincSecIndicator(FinSecIndicatorVO finSecIndicatorVOoBj) {
		// TODO Auto-generated method stub
		return finSecIndicatorVOoBj;
	}
}
