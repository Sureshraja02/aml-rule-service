package com.aml.srv.core.efrmsrv.rule.intr;

import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;

@Component
public interface RulesRiskComplianceIntr {

	public ComputedFactsVO ruleOfCountryRisk(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfCustomerMatch(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfFCRACompliance(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfPanStatus(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfAccountStatus(RuleRequestVo requVoObjParam, Factset factSetObj);
	
	public ComputedFactsVO ruleOfBeneficiaryRelation(RuleRequestVo requVoObjParam, Factset factSetObj);
	
}
