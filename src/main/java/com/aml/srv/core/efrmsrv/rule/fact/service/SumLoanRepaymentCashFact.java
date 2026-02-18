package com.aml.srv.core.efrmsrv.rule.fact.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;
import com.aml.srv.core.efrmsrv.rule.service.RulesIdentifierService;


@Service("SUM_LOAN_REPAYMENTS_CASHService")
public class SumLoanRepaymentCashFact implements FactInterface{


	private Logger LOGGER = LoggerFactory.getLogger(SumLoanRepaymentCashFact.class);
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::RulesExecutorService@ruleOfFDConversion (FD_CONVERSION) Called::::::::::",
				requVoObjParam.getReqId());
		try {

		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfFDConversion : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfFDConversion (FD_CONVERSION) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

}