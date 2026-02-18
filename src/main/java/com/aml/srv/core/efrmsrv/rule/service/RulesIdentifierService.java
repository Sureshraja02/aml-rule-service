package com.aml.srv.core.efrmsrv.rule.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrm.rule.fact.util.ClassLoaderUtil;
import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;
import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.repo.AccountDetailsService;
import com.aml.srv.core.efrmsrv.repo.AccountStatusRepositryImpl;
import com.aml.srv.core.efrmsrv.repo.CustomerDetailsRepoImpl;
import com.aml.srv.core.efrmsrv.rule.fact.service.FactInterface;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;
import com.aml.srv.core.efrmsrv.rule.process.response.RuleResponseVo;
import com.aml.srv.core.efrmsrv.rule.process.response.RuleResposeDetailsVO;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class RulesIdentifierService {

	private Logger LOGGER = LoggerFactory.getLogger(RulesIdentifierService.class);

	@Autowired
	RulesAggregateService rulesExecutorService;

	@Autowired
	RulesRiskComplianceService rulesRsikComplianceService;

	@Autowired
	CustomerDetailsRepoImpl customerDetailsRepoImpl;
	
	@Autowired
	AccountDetailsService accountDetailsService;
	
	@Autowired
	AccountStatusRepositryImpl accountStatusRepositryImpl;
	
	@Autowired
	RulesUtils rulesUtils;
	
	@Autowired
	ClassLoaderUtil classLoaderUtil;

	public RuleResposeDetailsVO toComputeAMLData(RuleRequestVo ruleRequestVoObParam) {

		String classname = RulesIdentifierService.class.getSimpleName();
		String methodname = Thread.currentThread().getStackTrace()[1].getMethodName();
		LOGGER.info("RulesIdentifierService toComputeAMLData method called......[{}] [{}]", classname, methodname);

		RuleResponseVo ruleResponseVoObj = null;
		List<RuleResposeDetailsVO> ruleRespDtlObj = null;
		RuleResposeDetailsVO ruleResposeDetailsVO = null;
		List<ComputedFactsVO> computedFacts = null;
		ComputedFactsVO computedFactsVO = null;
		boolean IMMEDIATE_WITHDRAWAL = false;
		try {
			LOGGER.info("RulesIdentifierService toComputeAMLData - ruleRequestVoObParam [{}]......",
					ruleRequestVoObParam);
			LOGGER.info("RulesIdentifierService toComputeAMLData - ruleRequestVoObParam as String[{}]......",
					ruleRequestVoObParam.toString());
			if (ruleRequestVoObParam != null) {
				ruleResponseVoObj = new RuleResponseVo();

				ruleRespDtlObj = new ArrayList<RuleResposeDetailsVO>();
				computedFacts = new ArrayList<ComputedFactsVO>();
				ruleResposeDetailsVO = new RuleResposeDetailsVO();
				for (Factset fact : ruleRequestVoObParam.getFactSet()) {
					computedFactsVO = new ComputedFactsVO();
					if (StringUtils.isNotBlank(fact.getFact())) {

						FactInterface factInterface = classLoaderUtil.getBean(fact.getFact() + "Service",
								FactInterface.class);
						computedFactsVO = factInterface.getFactExecutor(ruleRequestVoObParam, fact, computedFacts);
						computedFactsVO.setFact(fact.getFact());
						computedFactsVO.setFieldTag(fact.getField());
						
						
						computedFacts.add(computedFactsVO);
					} else {
						LOGGER.info("RuleRequestVo object is NULL recevie");
					}
				}
			}

			if (StringUtils.isNotBlank(ruleRequestVoObParam.getCustomerId())) {
				CustomerDetailsEntity customerEnityObj = customerDetailsRepoImpl
						.getCustomerDetailsByCustId(ruleRequestVoObParam.getCustomerId());
				if (customerEnityObj != null) {
					ruleResposeDetailsVO.setAccountType(customerEnityObj.getCustomerType());
				}
			}
			if (StringUtils.isNotBlank(ruleRequestVoObParam.getAccountNo())) {
				AccountDetailsEntity customerEnityObj = accountDetailsService.getAccountDetails(ruleRequestVoObParam.getReqId(),null, ruleRequestVoObParam.getAccountNo());
						
				if (customerEnityObj != null) {
					ruleResposeDetailsVO.setAccountType(customerEnityObj.getAccountType());
				}
			}
			
			
			
			if (StringUtils.isNotBlank(ruleRequestVoObParam.getAccountNo())) {
				AccountStatusEntity accountStatusEntityObj = accountStatusRepositryImpl
						.getAccountStatusByAccNO(ruleRequestVoObParam.getAccountNo(), ruleRequestVoObParam.getReqId());
				if (accountStatusEntityObj != null) {
					ruleResposeDetailsVO.setAccountStatus(accountStatusEntityObj.getStatus());
				}

			}
			ruleResposeDetailsVO.setComputedFacts(computedFacts);
			ruleResposeDetailsVO.setReqId(ruleRequestVoObParam.getReqId());

			// ruleRespDtlObj.add(ruleResposeDetailsVO);
			// ruleResponseVoObj.setRuleResponse(ruleRespDtlObj);

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesIdentifierService");
		} finally {

		}
		return ruleResposeDetailsVO;
	}
}
