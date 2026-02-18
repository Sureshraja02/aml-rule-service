package com.aml.srv.core.efrmsrv.cust.profiling.service;

import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;

public interface RuleFactInt {

	
	public Integer sumOfCumulative(RuleRequestVo ruleReqVOObj);
	
	public Integer highValue(RuleRequestVo ruleReqVOObj);
	
	public Integer turnOver(RuleRequestVo ruleReqVOObj);
	
	public double avgTurnOver(RuleRequestVo ruleReqVOObj); 
	
	public Integer countDRCR(RuleRequestVo ruleReqVOObj);
	
	public Integer avgAccActivity(RuleRequestVo ruleReqVOObj);
	
}
