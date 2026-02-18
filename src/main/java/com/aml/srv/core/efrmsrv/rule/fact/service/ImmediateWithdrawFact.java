package com.aml.srv.core.efrmsrv.rule.fact.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
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
import com.aml.srv.core.efrmsrv.rule.service.RulesUtils;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;


@Service("IMMEDIATE_WITHDRAWALService")
public class ImmediateWithdrawFact implements FactInterface{

    private final AMLConstants AMLConstants;

	@Autowired
	TransactionService transactionService;

	private Logger LOGGER = LoggerFactory.getLogger(ImmediateWithdrawFact.class);
	
	@Autowired
	RulesUtils rulesUtils;

    ImmediateWithdrawFact(AMLConstants AMLConstants) {
        this.AMLConstants = AMLConstants;
    }
	
	@Override
	public ComputedFactsVO getFactExecutor(RuleRequestVo requVoObjParam, Factset factSetObj,List<ComputedFactsVO> computedFacts ) {
		ComputedFactsVO computedFactsVOObj = null;
		LOGGER.info(
				"REQID : [{}]::::::::::::RulesExecutorService@ruleOfImmediateWithdraw (IMMEDIATE_WITHDRAWAL) Called::::::::::",
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
			
			if(condition!=null)
			{
			if (condition.equals("LARGE_DEPOSIT")) {
				TransactionDetailsDTO dto = transactionService.getTransactionDetails(reqId, custId, accNo, null, null,AMLConstants.DEPOSIT,
						transMode, days, months, factSetObj, range);
				if (dto != null && dto.getMaxAmount() != null) {

					computedFactsVOObj.setFact(factName);
					computedFactsVOObj.setValue((dto.getMaxAmount()));
					BigDecimal depositeLargerAmount = dto.getMaxAmount();
					String transDate = dto.getTransDate();
					computedFactsVOObj = rulesUtils.toGetWithdaralFacts(requVoObjParam.getReqId(),
							requVoObjParam.getAccountNo(), requVoObjParam.getCustomerId(),
							requVoObjParam.getTransactionMode(),AMLConstants.DEPOSIT,
							depositeLargerAmount, transDate, factName, factSetObj.getCondition(), factSetObj,
							null);
				}
			}
			else
			{

				String  fieldName = null;
				computedFactsVOObj = new ComputedFactsVO();
				fieldName = factSetObj.getField();
				factName = factSetObj.getFact();
				LOGGER.info("REQID : [{}] - fieldName : [{}] - factName : [{}]", requVoObjParam.getReqId(), fieldName,
						factName);
			 {
				
				 {
						if (computedFacts != null && computedFacts.size() >= 1) {
							/**
							 * Large Deposit
							 */
							Optional<ComputedFactsVO> matchedComputedFactsVOObj = computedFacts.stream()
									.filter(p -> p.getFact().equals(AMLConstants.LARGE_DEPOSIT)).findFirst();
							if (matchedComputedFactsVOObj.isPresent()) {
								LOGGER.info("Found: " + matchedComputedFactsVOObj.get());
								BigDecimal depositeLargerAmount = matchedComputedFactsVOObj.get().getValue();
								String transDate = matchedComputedFactsVOObj.get().getTransDate();
								computedFactsVOObj = rulesUtils.toGetWithdaralFacts(requVoObjParam.getReqId(),
										requVoObjParam.getAccountNo(), requVoObjParam.getCustomerId(),
										requVoObjParam.getTransactionMode(), requVoObjParam.getTxnType(),
										depositeLargerAmount, transDate, factName, factSetObj.getCondition(), factSetObj,
										matchedComputedFactsVOObj);
							} else {
								LOGGER.info(AMLConstants.LARGE_DEPOSIT + " not found.");
							}

							/**
							 * Sum of Cash Deposit
							 */
							Optional<ComputedFactsVO> matchedComputedFactsVOObjTwo = computedFacts.stream()
									.filter(p -> p.getFact().equals(AMLConstants.SUM_CASH_DEPOSITS)).findFirst();
							if (matchedComputedFactsVOObjTwo.isPresent()) {
								LOGGER.info("Found: ", matchedComputedFactsVOObjTwo.get());
								BigDecimal depositeLargerAmount = matchedComputedFactsVOObjTwo.get().getValue();
								String transDate = matchedComputedFactsVOObjTwo.get().getTransDate();
								computedFactsVOObj = rulesUtils.toGetWithdaralFacts(requVoObjParam.getReqId(),
										requVoObjParam.getAccountNo(), requVoObjParam.getCustomerId(),
										requVoObjParam.getTransactionMode(), requVoObjParam.getTxnType(),
										depositeLargerAmount, transDate, factName, factSetObj.getCondition(), factSetObj,
										matchedComputedFactsVOObjTwo);
							} else {
								LOGGER.info(AMLConstants.SUM_CASH_DEPOSITS + " not found.");
							}
						}
					}
				}
			
			}
			
			}
			
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesExecutorService@ruleOfImmediateWithdraw : {}", e);
		} finally {

			LOGGER.info(
					"REQID : [{}]::::::::::::RulesExecutorService@ruleOfImmediateWithdraw (IMMEDIATE_WITHDRAWAL) End::::::::::\n\n",
					requVoObjParam.getReqId());
		}
		return computedFactsVOObj;
	}

}