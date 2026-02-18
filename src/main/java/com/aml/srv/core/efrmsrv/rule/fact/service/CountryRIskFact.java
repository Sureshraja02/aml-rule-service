package com.aml.srv.core.efrmsrv.rule.fact.service;

import java.math.BigDecimal;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.FS_FIUIndHighRiskCountryEntity;
import com.aml.srv.core.efrmsrv.entity.FS_FIUIndTerrorLocationEntity;
import com.aml.srv.core.efrmsrv.repo.CustomerDetailsService;
import com.aml.srv.core.efrmsrv.repo.FS_FIUIndHighRiskCountryRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FIUIndTerrorLocationRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FactConditionAttributeRepoImpl;
import com.aml.srv.core.efrmsrv.repo.FS_FactConditionRepoImpl;
import com.aml.srv.core.efrmsrv.repo.TransactionDetailsDTO;
import com.aml.srv.core.efrmsrv.repo.TransactionService;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;


@Service("COUNTRY_RISKService")
public class CountryRIskFact implements FactInterface{

	
	@Autowired
	TransactionService transactionService;
	
	@Autowired
	FS_FactConditionRepoImpl fS_FactConditionRepoImpl;

	@Autowired
	FS_FactConditionAttributeRepoImpl fS_FactConditionAttributeRepoImpl;
	
	@Autowired
	CustomerDetailsService customerDetailsService;
	
	@Autowired
    FS_FIUIndHighRiskCountryRepoImpl fS_FIUIndHighRiskCountryRepoImpl;
   
	@Autowired
    FS_FIUIndTerrorLocationRepoImpl fS_FIUIndTerrorLocationRepoImpl;


	private Logger LOGGER = LoggerFactory.getLogger(SumCashTxnFact.class);
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::CountryRIskFact@getFactExecutor (ENTRY) Called::::::::::",
				requVoObjParam.getReqId());
		String factName = null, accNo = null, custId = null, transMode = null, transType = null, 
				txnTime = null, txnId = null, reqId = null;
		try {
			computedFactsVOObj = new ComputedFactsVO();
			accNo = requVoObjParam.getAccountNo();
			custId = requVoObjParam.getCustomerId();
			txnId = requVoObjParam.getTxnId();
			reqId = requVoObjParam.getReqId();
			transMode = requVoObjParam.getTransactionMode();
			transType = requVoObjParam.getTxnType();			
			factName = factSetObj.getFact();
			Integer days = factSetObj.getDays();
			Integer hours = factSetObj.getHours();
			Integer months = factSetObj.getMonths();
			txnTime = requVoObjParam.getTxn_time();
			Range range = factSetObj.getRange();
			String condition = factSetObj.getCondition();
			TransactionDetailsDTO dto =null;
			 computedFactsVOObj.setStrType("str");
			if(condition!=null)
			{
				 dto = transactionService.getTransactionDetails(reqId, custId, accNo, txnId, transType);
			 if(dto!=null && dto.getCounterContryCode()!=null)
			{
				
			if(condition!=null && condition.equals("HIGH_RISK_COUNTRIES"))
			{
					FS_FIUIndHighRiskCountryEntity	countryEntity = fS_FIUIndHighRiskCountryRepoImpl.getCountryByritiria(requVoObjParam.getReqId(), dto.getCounterContryCode());
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
			else if(condition!=null && condition.equals("HIGH_RISK"))
			{
					FS_FIUIndHighRiskCountryEntity	countryEntity = fS_FIUIndHighRiskCountryRepoImpl.getCountryByritiria(requVoObjParam.getReqId(), dto.getCounterContryCode());
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
			else if(condition!=null && condition.equals("TERROR_LOCATIONS"))
			{
				
				FS_FIUIndTerrorLocationEntity	terrorLocationEntity = fS_FIUIndTerrorLocationRepoImpl.getCountryByritiria(requVoObjParam.getReqId(), dto.getCounterContryCode());
				if(terrorLocationEntity!=null)
				{
					if(terrorLocationEntity.getCountry_Code()!=null && "IN".equals(terrorLocationEntity.getCountry_Code()))
					{
						if(dto.getCounterLocation().contains(terrorLocationEntity.getLocation()))
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
					
					
					else 
					{
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setStrValue("HIGH_RISK");	
					}
					
					
					
				}
				else
				{
					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setStrValue("NO_HIGH_RISK");	
				}
				
			}
			}
			}
			

			

		} catch (Exception e) {
			LOGGER.error("Exception found in CountryRIskFact@getFactExecutor : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::CountryRIskFact@getFactExecutor (EXIT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

}