package com.aml.srv.core.efrmsrv.rule.fact.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;
import com.aml.srv.core.efrmsrv.repo.AccountDetailsService;
import com.aml.srv.core.efrmsrv.repo.TransactionDetailsDTO;
import com.aml.srv.core.efrmsrv.repo.TransactionService;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.request.RuleRequestVo;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;


@Service("INACTIVITY_PERIODService")
public class InActivityPeriodFact implements FactInterface{

private Logger LOGGER = LoggerFactory.getLogger(SumDebitCreditFact.class);
	
	@Autowired
	TransactionService transactionService;
	
	@Autowired
	AccountDetailsService accountDetailsService;
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {

		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info("REQID : [{}]::::::::::::AccountStatusFact@getFactExecutor (ENTRY) Called::::::::::",
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
			computedFactsVOObj.setFact(factName);
			if (condition != null) {
				if (condition.equals("NEW_ACCOUNT")) {
					AccountDetailsEntity acctDetails = accountDetailsService
							.getAccountDetails(requVoObjParam.getReqId(), accNo, custId);
					if (acctDetails != null && acctDetails.getAccountOpenedDate() != null) {
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
						LocalDate openDate = LocalDate.parse(acctDetails.getAccountOpenedDate(), formatter);
						LocalDate currentDate = LocalDate.now();
						System.out.println(openDate); // Output: 2025-05-20
						computedFactsVOObj.setStrType("str");
						long daysBetween = ChronoUnit.DAYS.between(openDate, currentDate);
						if (days != null && days >= daysBetween) {
							computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
							computedFactsVOObj.setStrValue("NEW");
						} else if (months != null) {
							int totalDays = months * 30;
							if (totalDays >= daysBetween) {
								computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
								computedFactsVOObj.setStrValue("NEW");
							}
						} else {
							computedFactsVOObj.setAcc_open_date(acctDetails.getAccountOpenedDate());
							computedFactsVOObj.setStrValue("OLD");
						}
					} else {

						computedFactsVOObj.setStrValue("OLD");
					}

				}
				
					

			}
			else
			{
				
				AccountStatusEntity acctStatus=accountDetailsService.getAccountStatusByAccNO(accNo, reqId);
			
			if (acctStatus != null && acctStatus.getStatus() != null) {

				computedFactsVOObj.setFact(factName);
				computedFactsVOObj.setStrValue(acctStatus.getStatus());
				computedFactsVOObj.setStrType("str");
			}
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in AccountStatusFact@getFactExecutor : {}", e);
		} finally {

			LOGGER.info("REQID : [{}]::::::::::::AccountStatusFact@getFactExecutor (EXIT) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;

	}

}