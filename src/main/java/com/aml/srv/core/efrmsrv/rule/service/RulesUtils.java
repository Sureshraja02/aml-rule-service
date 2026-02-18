package com.aml.srv.core.efrmsrv.rule.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;
import com.aml.srv.core.efrmsrv.utils.AMLConstants;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class RulesUtils {

	private Logger LOGGER = LoggerFactory.getLogger(RulesUtils.class);

	@Autowired
	EntityManager entityManager;

	public ComputedFactsVO toGetWithdaralFacts(String reqId, String accountNo,
			String custmerId, String transMode, String transType, BigDecimal depositeLargerAmount, 
			String transDate, String factName, String conditianName, Factset factSetObj, Optional<ComputedFactsVO> computedFactsVOParam) {
		LOGGER.info("REQ ID : [{}] - toGetWithdaralFacts Method Called........", reqId);
		ComputedFactsVO computedFactsVO = null;
		PercentageDetailsVO percentageDetailsVOObj = null;
		try {
			computedFactsVO = new ComputedFactsVO();
			LOGGER.info("REQ ID : [{}] - toGetWithdaralFacts conditianName : [{}]........", conditianName);
			percentageDetailsVOObj = toGetValueByImediateWithDraw(reqId, accountNo, custmerId,transMode,transType, depositeLargerAmount, transDate, factName,conditianName,factSetObj,computedFactsVOParam);
			if (percentageDetailsVOObj != null) {
				if (StringUtils.isNotBlank(conditianName)) {
					switch (conditianName) {
					case AMLConstants.IMMEDIATE_DIFFERENT_LOCATIONS:
						computedFactsVO.setFact(factName);
						computedFactsVO.setValue(percentageDetailsVOObj.getTotalValue());
						computedFactsVO.setPerCentValue(String.valueOf(toGetPercentAge(percentageDetailsVOObj)));
						break;
					case AMLConstants.IMMEDIATE_WITHDRAWAL:
						computedFactsVO.setFact(factName);
						computedFactsVO.setValue(percentageDetailsVOObj.getTotalValue());
						computedFactsVO.setPerCentValue(String.valueOf(toGetPercentAge(percentageDetailsVOObj)));
						break;
					case AMLConstants.IMMEDIATE_WITHDRAWAL_ATM_OR_OTHER:
						computedFactsVO.setFact(factName);
						computedFactsVO.setValue(percentageDetailsVOObj.getTotalValue());
						computedFactsVO.setPerCentValue(String.valueOf(toGetPercentAge(percentageDetailsVOObj)));
						break;
					default:
						LOGGER.info("REQ ID : [{}] - toGetWithdaralFacts default block Condition not match");
						computedFactsVO.setFact(factName);
						computedFactsVO.setValue(percentageDetailsVOObj.getTotalValue());
					}
				} else {
					LOGGER.info("REQ ID : [{}] - toGetWithdaralFacts conditation not available.");
					computedFactsVO.setFact(factName);
					computedFactsVO.setValue(percentageDetailsVOObj.getTotalValue());
				}
			}
		} catch (Exception e) {
			computedFactsVO = null;
			LOGGER.error("Exception found in toGetWithdaralFacts : {}", e);
		} finally {
			LOGGER.info("REQ ID : [{}] - toGetWithdaralFacts Method End........", reqId);
		}
		return computedFactsVO;
	}

	/**
	 * 
	 * @param reqId
	 * @param accountNo
	 * @param custmerId
	 * @param depositeLargerAmount
	 * @param transDate
	 * @param factName
	 * @param columnName
	 * @return toGetValueByImediateWithDraw ComputedFactsVO
	 */
	public PercentageDetailsVO toGetValueByImediateWithDraw(String reqId, String accountNo, String custmerId, String transMode, String transType, BigDecimal depositeLargerAmount, String transDate,
			String factName, String conditianName, Factset factSetObj, Optional<ComputedFactsVO> computedFactsVOParam) {
		LOGGER.info("REQ ID : [{}] - toGetValueByImediateWithDraw Method Called............", reqId);
		PercentageDetailsVO percentageDetailsVOObj = null;
		BigDecimal retnVal = null;
		
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			percentageDetailsVOObj = new PercentageDetailsVO();
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			
			if (StringUtils.isNotBlank(conditianName)) {
				switch (conditianName) {
				case AMLConstants.IMMEDIATE_DIFFERENT_LOCATIONS:
					LOGGER.debug("REQID : [{}] - Transaction Date : [{}] - Deposite amt : [{}]", reqId, transDate, depositeLargerAmount);
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("transactionDate"), transDate));
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), depositeLargerAmount));
					//
					break;
				case AMLConstants.IMMEDIATE_WITHDRAWAL:
					LOGGER.debug("REQID : [{}] - Transaction Date : [{}] - Deposite amt : [{}]", reqId, transDate, depositeLargerAmount);
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("transactionDate"), transDate));
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), depositeLargerAmount));
					break;
				case AMLConstants.IMMEDIATE_WITHDRAWAL_ATM_OR_OTHER:
					LOGGER.debug("REQID : [{}] - Transaction Date : [{}] - Deposite amt : [{}]", reqId, transDate, depositeLargerAmount);
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("transactionDate"), transDate));
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), depositeLargerAmount));
					break;
				default:
					LOGGER.info("REQ ID : [{}] - toGetValueByImediateWithDraw default block Condition not match");
				}
			} else {
				if(StringUtils.isNotBlank(transDate)) {
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("transactionDate"), transDate));
				}
				predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), depositeLargerAmount));
			}
			
			/**
			 * Account Number 
			 */
			if (StringUtils.isNotBlank(accountNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accountNo));
			}
			/**
			 * Customer Id 
			 */
			if (StringUtils.isNotBlank(custmerId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custmerId));
			}
			/**
			 * Transaction Type 
			 */
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
			} else {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "W"));
			}

			/**
			 * Transaction Mode 
			 */
			if (StringUtils.isNotBlank(transMode)) {
				predicates.add(cb.greaterThanOrEqualTo(rootBk.get("channelType"), transMode));
			}
			/*else {
				List<String> type = Arrays.asList("CASH", "CHEQUE");
				Predicate inClause = rootBk.get("channelType").in(type);
				predicates.add(inClause);
			}*/
			
			if (factSetObj.getRange() != null) {
				if (factSetObj.getRange().getMin() != null && factSetObj.getRange().getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), factSetObj.getRange().getMin(), factSetObj.getRange().getMax()));
				} else if (factSetObj.getRange().getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), factSetObj.getRange().getMin()));
				} else if (factSetObj.getRange().getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), factSetObj.getRange().getMax()));
				}

			}

			cq.where(cb.and(predicates.toArray(new Predicate[0])));
			cq.multiselect(cb.count(rootBk), cb.max(rootBk.get("amount")));
			Object[] result = entityManager.createQuery(cq).getSingleResult();
			if (result != null && result.length > 1) {
				percentageDetailsVOObj.setReqId(reqId);
				percentageDetailsVOObj.setNoOfTimes((Long) result[0]);
				percentageDetailsVOObj.setTotalValue((BigDecimal) result[1]);
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
			} else {
				percentageDetailsVOObj = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
			}
			
		} catch (Exception e) {
			percentageDetailsVOObj = null;
			LOGGER.error("REQ ID : [{}] - Exception found in RulesUtils@toGetValueByImediateWithDraw : {}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
		}
		LOGGER.info("REQ ID : [{}] - toGetValueByImediateWithDraw Method End............\n\n", reqId);
		return percentageDetailsVOObj;
	}

	/**
	 * 
	 * @param reqId
	 * @param noOfTimes
	 * @param totalValue
	 * @return toGetPercentAge BigDecimal
	 */
	public BigDecimal toGetPercentAge(PercentageDetailsVO percentageDetailsObj) {
		LOGGER.info("REQ ID : [{}] - toGetPercentAge Method Called............", percentageDetailsObj.getReqId());
		BigDecimal perCentageValue = null;
		try {
			
			BigDecimal noOfTimes = new BigDecimal(percentageDetailsObj.getNoOfTimes());
			BigDecimal totalValue = percentageDetailsObj.getTotalValue();

			perCentageValue = noOfTimes
			    .divide(totalValue, 10, RoundingMode.HALF_UP)
			    .multiply(BigDecimal.valueOf(100)).setScale(2, RoundingMode.HALF_UP);
			
			/*
			 * perCentageValue =
			 * percentageDetailsObj.getNoOfTimes().divide(BigDecimal.valueOf(
			 * percentageDetailsObj.getTotalValue().longValue()), 10, RoundingMode.HALF_UP)
			 * // scale 10 for precision .multiply(new BigDecimal("100"));
			 */

		} catch (Exception e) {
			LOGGER.error("REQ ID : [{}] - Exception found in RulesUtils@toGetPercentAge : {}", percentageDetailsObj.getReqId(), e);
		} finally {
			LOGGER.info("REQ ID : [{}] - toGetPercentAge Method End............\n\n", percentageDetailsObj.getReqId());
		}
		return perCentageValue;
	}

	public boolean toCheckObjectList(List<ComputedFactsVO> computedFacts, String largeDeposit) {

		boolean finalCheck = false;
		try {

			boolean factNameexists = computedFacts.stream().filter(Objects::nonNull) // avoid NullPointerException
					.map(ComputedFactsVO::getFact).anyMatch(largeDeposit::equals);

			/*
			 * boolean factNameexists2 = factSetResp.stream().filter(Objects::nonNull) //
			 * avoid NullPointerException
			 * .map(ComputedFactsVO::getFact).anyMatch(factName2::equals);
			 */

			if (factNameexists) {
				finalCheck = true;
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in RulesUtils@toCheckObjectList : {}", e);
		} finally {

		}
		// TODO Auto-generated method stub
		return finalCheck;
	}

	

}
