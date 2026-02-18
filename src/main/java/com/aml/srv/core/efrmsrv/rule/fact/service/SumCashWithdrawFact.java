package com.aml.srv.core.efrmsrv.rule.fact.service;

import java.math.BigDecimal;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.repo.TransactionDetailsDTO;
import com.aml.srv.core.efrmsrv.repo.TransactionService;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;
import com.aml.srv.core.efrmsrv.rule.service.RulesIdentifierService;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;


@Service("SUM_CASH_WITHDRAWALSService")
public class SumCashWithdrawFact implements FactInterface{


private Logger LOGGER = LoggerFactory.getLogger(SumDebitCreditFact.class);
	
	@Autowired
	TransactionService transactionService;
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::SumCashWithdrawFact@getFactExecutor (ENTRY) Called::::::::::",
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

			TransactionDetailsDTO dto = transactionService.getTransactionDetails(reqId, custId, accNo, null, null,AMLConstants.WITHDRAW,
					transMode, days, months, factSetObj, range);
			computedFactsVOObj.setStrType("num");
			if (dto != null && dto.getSumAmount() != null) {

				computedFactsVOObj.setFact(factName);
				computedFactsVOObj.setValue((dto.getSumAmount()));
			}
			else
			{
				computedFactsVOObj.setFact(factName);
				computedFactsVOObj.setValue(new BigDecimal(0));
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in SumCashWithdrawFact@getFactExecutor : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::SumCashWithdrawFact@getFactExecutor (EXIT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

}