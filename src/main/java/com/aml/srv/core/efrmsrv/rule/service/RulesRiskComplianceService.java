package com.aml.srv.core.efrmsrv.rule.service;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.FS_FIUIndHighRiskCountryEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FIUIndTerrorLocationEntity;
import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.repo.CustomerDetailsRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FIUIndHighRiskCountryRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FIUIndTerrorLocationRepoImpl;
import com.aml.srv.core.efrmsrv.repo.TransactionDetailsRepositryImpl2;
import com.aml.srv.core.efrmsrv.rule.intr.RulesRiskComplianceIntr;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class RulesRiskComplianceService implements RulesRiskComplianceIntr {

	@Autowired
     FS_FIUIndHighRiskCountryRepoImpl fS_FIUIndHighRiskCountryRepoImpl;
    
	@Autowired
     FS_FIUIndTerrorLocationRepoImpl fS_FIUIndTerrorLocationRepoImpl;

	private Logger LOGGER = LoggerFactory.getLogger(RulesRiskComplianceService.class);
	
	@Autowired
	TransactionDetailsRepositryImpl2 transactionDetailsRepositryImpl2;
	
	@Autowired
	CustomerDetailsRepoImpl customerDetailsRepoImpl;

   

	@Override
	public ComputedFactsVO ruleOfCountryRisk(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfCountryRisk (HIGH_RISK_COUNTRY) Called::::::::::", requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null, txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			String txnId=requVoObjParam.getTxnId();
			String condition=factSetObj.getCondition();
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				
				
				TransactionDetailsEntity txnDetails=transactionDetailsRepositryImpl2.getTxnDetails(tableName, accNo, custId, transMode, transType, hours, days, months, fieldName, columnName, factSetObj.getRange(), txnId);
				if(txnDetails!=null && txnDetails.getCounterCountryCode()!=null)
				{
				if(condition!=null && condition.equals("HIGH_RISK_COUNTRIES"))
				{
						FS_FIUIndHighRiskCountryEntity	countryEntity = fS_FIUIndHighRiskCountryRepoImpl.getCountryByritiria(requVoObjParam.getReqId(), txnDetails.getCounterCountryCode());
						if(countryEntity!=null)
						{
							computedFactsVOObj.setFact(factName);
							computedFactsVOObj.setStrValue("HIGH_RISK");	
						}
						else
						{
							computedFactsVOObj.setFact(factName);
							computedFactsVOObj.setStrValue("NO_HIGH_RISK");	
						}
						
					
				}
				else if(condition!=null && condition.equals("TERROR_HIGH_RISK"))
				{
					FS_FIUIndTerrorLocationEntity	countryEntity = fS_FIUIndTerrorLocationRepoImpl.getCountryByritiria(requVoObjParam.getReqId(), txnDetails.getCounterCountryCode());
					if(countryEntity!=null)
					{
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setStrValue("TERROR_HIGH_RISK");	
					}
					else
					{
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setStrValue("NO_TERROR_HIGH_RISK");	
					}
				}
				else
				{
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setStrValue("NO_RISK");	
				}
				
				}
				else
				{
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setStrValue("NO_RISK");	
				}
				
			
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfCountryRisk : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfCountryRisk (HIGH_RISK_COUNTRY) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfCustomerMatch(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfCustomerMatch (CUSTOMER_MATCH) Called::::::::::", requVoObjParam.getReqId());
		try {
			
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfCustomerMatch : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfCustomerMatch (CUSTOMER_MATCH) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfFCRACompliance(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfFCRACompliance (FCRA_COMPLIANCE) Called::::::::::", requVoObjParam.getReqId());
		try {
			
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfFCRACompliance : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfFCRACompliance (FCRA_COMPLIANCE) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfPanStatus(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfPanStatus (PAN_STATUS) Called::::::::::", requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null, txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			String finalValue = null;
					finalValue = customerDetailsRepoImpl.getPanStatus(requVoObjParam.getReqId(), accNo, custId, transMode, transType, hours, days, months, fieldName,factSetObj.getRange());
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setStrValue(finalValue);
				
			

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfPanStatus : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfPanStatus (PAN_STATUS) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfAccountStatus(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfAccountStatus (ACCOUNT_STATUS) Called::::::::::", requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, fieldName = null, txnTime = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();
			fieldName = factSetObj.getField();
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			String finalValue = null;
			if (StringUtils.isNotBlank(fieldName) && fieldName.contains(".")) {
				String tableName = fieldName.split("\\.")[0];
				String columnName = fieldName.split("\\.")[1];
				if (StringUtils.isNotBlank(tableName) && tableName.equalsIgnoreCase("ACCOUNT")) {
					finalValue = transactionDetailsRepositryImpl2.getAccountStatus(requVoObjParam.getReqId(), accNo, custId, transMode, transType, hours, days, months, fieldName, columnName,factSetObj.getRange(),factSetObj);
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setStrValue(finalValue);
				}
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfAccountStatus : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfAccountStatus (ACCOUNT_STATUS) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

	@Override
	public ComputedFactsVO ruleOfBeneficiaryRelation(RuleRequestVo requVoObjParam, Factset factSetObj) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfBeneficiaryRelation (BENEFICIARY_RELATION) Called::::::::::", requVoObjParam.getReqId());
		try {
			
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesRiskComplianceService@ruleOfBeneficiaryRelation : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::RulesRiskComplianceService@ruleOfBeneficiaryRelation (BENEFICIARY_RELATION) End::::::::::\n\n", requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

}
