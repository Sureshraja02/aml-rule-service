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
import com.aml.srv.core.efrmsrv.utils.AMLConstants;


@Service("WITHDRAWAL_PERCENTAGEService")
public class WithdrawPercentageFact implements FactInterface{



private Logger LOGGER = LoggerFactory.getLogger(SumDebitCreditFact.class);
	
	@Autowired
	TransactionService transactionService;
	
	@Autowired
	ImmediateWithdrawFact immediateWithdrawFact;
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::WithdrawPercentageFact@getFactExecutor (ENTRY) Called::::::::::",
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
			
			computedFactsVOObj.setStrType("num");
			if (condition != null) {
				if (condition.equals("QUARTERLY_PERCENTAGE")) {


					TransactionDetailsDTO threeMonthsumofamount = transactionService.getTransactionDetails(reqId, custId, accNo, null, null,null,
							null, null, 3, factSetObj, range,true);
					
					TransactionDetailsDTO threeMonthsumOfwithdraw = transactionService.getTransactionDetails(reqId, custId, accNo, txnId, null,AMLConstants.WITHDRAW,
							transMode, days, months, factSetObj, range,true);
					
					if (threeMonthsumofamount != null && threeMonthsumofamount.getSumAmount() != null && threeMonthsumOfwithdraw!=null && threeMonthsumOfwithdraw.getSumAmount()!=null) {
						Double withdrawalAmount = threeMonthsumOfwithdraw.getSumAmount().doubleValue();
				        Double totalAmount = threeMonthsumofamount.getSumAmount().doubleValue();

				        double percentage = (withdrawalAmount / totalAmount) * 100;
				        computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setValue(new BigDecimal(percentage));
					}
					else
					{
						computedFactsVOObj.setFact(factName);
						computedFactsVOObj.setValue(new BigDecimal(0));
					}
					

				}
				else if (condition.equals("IMMEDIATE_WITHDRAWAL_OTHER_ATM")) {


					if (computedFacts != null && computedFacts.size() >= 1) {
						computedFactsVOObj = immediateWithdrawFact.getFactExecutor(requVoObjParam,
								factSetObj, computedFacts);
					}
					

				}
				else if (condition.equals("IMMEDIATE_WITHDRAWAL_ATM_OR_OTHER")) {

					if (computedFacts != null && computedFacts.size() >= 1) {
						computedFactsVOObj = immediateWithdrawFact.getFactExecutor(requVoObjParam, factSetObj,
								computedFacts);
					}
				} else if (condition.equals("IMMEDIATE_DIFFERENT_LOCATIONS")) {

					if (computedFacts != null && computedFacts.size() >= 1) {
						computedFactsVOObj = immediateWithdrawFact.getFactExecutor(requVoObjParam, factSetObj,
								computedFacts);
					}

				}
				 else if (condition.equals("IMMEDIATE_WITHDRAWAL")) {

						if (computedFacts != null && computedFacts.size() >= 1) {
							computedFactsVOObj = immediateWithdrawFact.getFactExecutor(requVoObjParam, factSetObj,
									computedFacts);
						}

					}
				

			}
			else
			{
				if (computedFacts != null && computedFacts.size() >= 1) {
					computedFactsVOObj = immediateWithdrawFact.getFactExecutor(requVoObjParam,
							factSetObj, computedFacts);
				}
				
		/*	TransactionDetailsDTO sumofamount = transactionService.getTransactionDetails(reqId, custId, accNo, null, null,null,
					null, days, months, factSetObj, range);
			
			TransactionDetailsDTO sumOfwithdraw = transactionService.getTransactionDetails(reqId, custId, accNo, txnId, null,AMLConstants.WITHDRAW,
					transMode, days, months, factSetObj, range);
			
			if (sumofamount != null && sumofamount.getSumAmount() != null && sumOfwithdraw!=null && sumOfwithdraw.getSumAmount()!=null) {
				Double withdrawalAmount = sumOfwithdraw.getSumAmount().doubleValue();
		        Double totalAmount = sumofamount.getSumAmount().doubleValue();

		        double percentage = (withdrawalAmount / totalAmount) * 100;
		        computedFactsVOObj.setFact(factName);
				computedFactsVOObj.setValue(new BigDecimal(percentage));
			}
			else
			{
				computedFactsVOObj.setFact(factName);
				computedFactsVOObj.setValue(new BigDecimal(0));
			}
			}
			
			*/
			}

			

		} catch (Exception e) {
			LOGGER.error("Exception found in WithdrawPercentageFact@getFactExecutor : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::WithdrawPercentageFact@getFactExecutor (EXIT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

}