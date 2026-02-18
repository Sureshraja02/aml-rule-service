package com.aml.srv.core.efrmsrv.repo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Service
public class TransactionService {

	private Logger LOGGER = LoggerFactory.getLogger(TransactionService.class);

	@Autowired
	EntityManager entityManager;

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info("REQID : [{}] - custId [{}]  ", reqId, custId);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));

				LOGGER.info("REQID : [{}] - columnName is :", reqId);
				if (predicates != null) {
					cq.where(cb.and(predicates.toArray(new Predicate[0])));
					cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
							cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")),rootBk.get("counterpartyAccountNo"));
					Object[] result = entityManager.createQuery(cq).getSingleResult();
					if (result != null && result.length > 1) {
						Long count = (Long) result[1];
						BigDecimal sum = (BigDecimal) result[2];
						BigDecimal min = (BigDecimal) result[3];
						BigDecimal max = (BigDecimal) result[4];
						Double avg = (Double) result[5];
						Long counterAccNo = (Long) result[6];
						dto.setCountAmount(count);
						dto.setMaxAmount(max);
						dto.setMinAmount(min);
						dto.setSumAmount(sum);
						dto.setAvgAmount(avg);
						dto.setCounterAccountNo(counterAccNo);
						LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
					} else {
						dto = null;
						LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
					}
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info("REQID : [{}] - custId [{}] accNo : [{}]  ", reqId, custId, account);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
				if (StringUtils.isNotBlank(account)) {
					predicates.add(cb.equal(rootBk.get("accountNo"), account));
				}

				LOGGER.info("REQID : [{}] - columnName is :", reqId);
				if (predicates != null) {
					cq.where(cb.and(predicates.toArray(new Predicate[0])));
					cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
							cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")));
					Object[] result = entityManager.createQuery(cq).getSingleResult();
					if (result != null && result.length > 1) {
						Long count = (Long) result[1];
						BigDecimal sum = (BigDecimal) result[2];
						BigDecimal min = (BigDecimal) result[3];
						BigDecimal max = (BigDecimal) result[4];
						Double avg = (Double) result[5];
						dto.setCountAmount(count);
						dto.setMaxAmount(max);
						dto.setMinAmount(min);
						dto.setSumAmount(sum);
						dto.setAvgAmount(avg);

						LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
					} else {
						dto = null;
						LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
					}
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info("REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] ", reqId, custId, account, txnId);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
				if (StringUtils.isNotBlank(account)) {
					predicates.add(cb.equal(rootBk.get("accountNo"), account));
				}
				if (StringUtils.isNotBlank(txnId)) {
					predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
				}

				LOGGER.info("REQID : [{}] - columnName is :", reqId);
				if (predicates != null) {
					cq.where(cb.and(predicates.toArray(new Predicate[0])));
					cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
							cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")));
					Object[] result = entityManager.createQuery(cq).getSingleResult();
					if (result != null && result.length > 1) {
						Long count = (Long) result[1];
						BigDecimal sum = (BigDecimal) result[2];
						BigDecimal min = (BigDecimal) result[3];
						BigDecimal max = (BigDecimal) result[4];
						Double avg = (Double) result[5];
						dto.setCountAmount(count);
						dto.setMaxAmount(max);
						dto.setMinAmount(min);
						dto.setSumAmount(sum);
						dto.setAvgAmount(avg);

						LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
					} else {
						dto = null;
						LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
					}
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType, String transactionMode, Integer days, Integer months, Factset factSetObj) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String accNo, String txnType,
			String transactionMode, Integer days, Integer months, Factset factSetObj, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, accNo, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType, String transactionMode, Integer days, Integer months, Factset factSetObj, Range range,Integer hours) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("ATM")) {
				List<String> channeltype = Arrays.asList("ATM");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}
			
			

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (hours != null) {
				LocalDateTime currentDateTime = LocalDateTime.now();
				// Subtract hours
				LocalDateTime stDateTime = currentDateTime.minusHours(hours);
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String todayStr = currentDateTime.format(formatter); 
				String startDateStr = stDateTime.format(formatter);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
		
				 }
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")), cb.countDistinct(rootBk.get("counterpartyAccountNo")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					Long counterpartyAccountNo = (Long) result[6];
				//	BigDecimal txnAmount = (BigDecimal) result[7];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);
					dto.setCOuntDistcounterpartyAccountNo(counterpartyAccountNo);
					//dto.setTxnAmount(txnAmount);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}
	
	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType,String deposiwithdrawal, String transactionMode,boolean foreignCountryCode, Integer days, Integer months, Factset factSetObj, Range range) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			if (StringUtils.isNotBlank(deposiwithdrawal)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), deposiwithdrawal));
			}
			
			
			if (foreignCountryCode) {
				List<String> countryCode = Arrays.asList("IN");
				Predicate incountryCode = (rootBk.get("counterCountryCode").in(countryCode));
				Predicate notInClause = cb.not(incountryCode);
				predicates.add(notInClause);

			}
			
			

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")), cb.countDistinct(rootBk.get("counterpartyAccountNo")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					Long counterpartyAccountNo = (Long) result[6];
					//BigDecimal txnAmount = (BigDecimal) result[7];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);
					dto.setCOuntDistcounterpartyAccountNo(counterpartyAccountNo);
					//dto.setTxnAmount(txnAmount);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}
	
	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType,String deposiwithdrawal, String transactionMode, Integer days, Integer months, Factset factSetObj, Range range) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			if (StringUtils.isNotBlank(deposiwithdrawal)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), deposiwithdrawal));
			}
			
			
			
			
			

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				
				
				Expression<Double> maxOf = cb.max(rootBk.get("amount"));
				Expression<Long> countOf = cb.count(rootBk);
				Expression<String> maxDateExp = cb.greatest(rootBk.<String>get("transactionDate"));
				// Expression<String> addressExp = rootBk.get("counterCountryCode");

			
				

				
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")), cb.countDistinct(rootBk.get("counterpartyAccountNo")), maxDateExp);
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					Long counterpartyAccountNo = (Long) result[6];
					//BigDecimal txnAmount = (BigDecimal) result[7];
					String txnDate=(String) result[7];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);
					dto.setCOuntDistcounterpartyAccountNo(counterpartyAccountNo);
					//dto.setTxnAmount(txnAmount);
					dto.setTransDate(txnDate);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}
	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType,String deposiwithdrawal, String transactionMode, Integer days, Integer months, Factset factSetObj, Range range,boolean skipCurrentMonth) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			if (StringUtils.isNotBlank(deposiwithdrawal)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), deposiwithdrawal));
			}
			
			

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if(skipCurrentMonth)
			{
				if (months != null) {
					LocalDate currentDateTdy = LocalDate.now();
					LocalDate MinuscurrentDateTdy = currentDateTdy.minusMonths(1);
					
					LocalDate fromDate = MinuscurrentDateTdy.minusMonths(months);
					// Convert LocalDate to String in same format as DB
					String todayStr = MinuscurrentDateTdy.toString(); // yyyy-MM-dd
					String startDateStr = fromDate.toString();
					LOGGER.info("REQID : [{}] skip Current Month - From Date : [{}]  toDate : [{}]", reqId, fromDate,
							MinuscurrentDateTdy);
					LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
							startDateStr);
					Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
							rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
					Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
							java.sql.Date.valueOf(todayStr));
					predicates.add(betweenDates);
				}
			}
			else
			{
				if (months != null) {
					LocalDate currentDateTdy = LocalDate.now();
					LocalDate stDate = currentDateTdy.minusMonths(months);
					// Convert LocalDate to String in same format as DB
					String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
					String startDateStr = stDate.toString();

					LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
							startDateStr);
					Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
							rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
					Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
							java.sql.Date.valueOf(todayStr));
					predicates.add(betweenDates);
				}
			}
		
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")), cb.sum(rootBk.get("amount")),
						cb.min(rootBk.get("amount")), cb.max(rootBk.get("amount")),cb.avg(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long count = (Long) result[1];
					BigDecimal sum = (BigDecimal) result[2];
					BigDecimal min = (BigDecimal) result[3];
					BigDecimal max = (BigDecimal) result[4];
					Double avg = (Double) result[5];
					dto.setCountAmount(count);
					dto.setMaxAmount(max);
					dto.setMinAmount(min);
					dto.setSumAmount(sum);
					dto.setAvgAmount(avg);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}

	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId, String txnType) {


		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] ",
				reqId, custId, account, txnId, txnType);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<TransactionDetailsEntity> cq = null;
		TypedQuery<TransactionDetailsEntity> query = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(TransactionDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			
			

			
			

			
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				query = entityManager.createQuery(cq);
				TransactionDetailsEntity txnDetails = query.getSingleResult();
			
				if (txnDetails != null) {
					dto.setCounterContryCode(txnDetails.getCounterCountryCode());
					dto.setCounterLocation(txnDetails.getCounterpartyAddress());
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	
	}
	
	public TransactionDetailsDTO getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType,String deposiwithdrawal, String transactionMode,boolean foreignCountryCode, Integer days, Integer months, Factset factSetObj, Range range,boolean amountOnly) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			if (StringUtils.isNotBlank(deposiwithdrawal)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), deposiwithdrawal));
			}
			
			
			if (foreignCountryCode) {
				List<String> countryCode = Arrays.asList("IN");
				Predicate incountryCode = (rootBk.get("counterCountryCode").in(countryCode));
				Predicate notInClause = cb.not(incountryCode);
				predicates.add(notInClause);

			}
			
			

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				
				
				cq.multiselect(rootBk.get("amount"));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
				
					BigDecimal txnAmount = (BigDecimal) result[1];
					
					dto.setTxnAmount(txnAmount);

					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, dto);
				} else {
					dto = null;
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return dto;

	}
	public List<TransactionDetailsEntity> getTransactionDetails(String reqId, String custId, String account, String txnId,
			String txnType,String deposiwithdrawal, String transactionMode,boolean foreignCountryCode, Integer days, Integer months, Factset factSetObj, Range range,Integer hours) {

		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);

		TransactionDetailsDTO dto = new TransactionDetailsDTO();
		List<TransactionDetailsEntity> list=null;
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
		LOGGER.info(
				"REQID : [{}] - custId [{}] accNo : [{}] txnId [{}] transType : [{}] transactionMode [{}]  days : [{}] months [{}]",
				reqId, custId, account, txnId, txnType, transactionMode, days, months);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<TransactionDetailsEntity> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(TransactionDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(account)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), account));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			if (StringUtils.isNotBlank(txnType)) {
				predicates.add(cb.equal(rootBk.get("transactionType"), txnType));
			}
			
			if (StringUtils.isNotBlank(deposiwithdrawal)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), deposiwithdrawal));
			}
			
			
			if (foreignCountryCode) {
				List<String> countryCode = Arrays.asList("IN");
				Predicate incountryCode = (rootBk.get("counterCountryCode").in(countryCode));
				Predicate notInClause = cb.not(incountryCode);
				predicates.add(notInClause);

			}
			
			

			if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			} else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("NON-CASH")) {
				List<String> channeltype = Arrays.asList("ATM", "CASH");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				Predicate notInClause = cb.not(inchanneltype);
				predicates.add(notInClause);

			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("ATM")) {
				List<String> channeltype = Arrays.asList("ATM");
				Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
				predicates.add(inchanneltype);
			}
			else if (StringUtils.isNotBlank(transactionMode) && transactionMode.equals("BOTH")) {
				
			}
			
			if (range != null) {
				if (range.getMin() != null && range.getMax() != null) {
					predicates.add(cb.between(rootBk.get("amount"), range.getMin(), range.getMax()));
				} else if (range.getMin() != null) {
					// Only min present → greaterThanOrEqualTo
					predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"), range.getMin()));
				} else if (range.getMax() != null) {
					// Only max present → lessThanOrEqualTo
					predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"), range.getMax()));
				}

			}

			if (days != null) {

				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			if (hours != null) {
				LocalDateTime currentDateTime = LocalDateTime.now();
				// Subtract hours
				LocalDateTime stDateTime = currentDateTime.minusHours(hours);
				
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String todayStr = currentDateTime.format(formatter); 
				String startDateStr = stDateTime.format(formatter);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
		
				 }
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();

				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				
				
				cq.multiselect(rootBk.get("narration"));
				//Object[] result = entityManager.createQuery(cq).getSingleResult();
				TypedQuery<TransactionDetailsEntity> query = entityManager.createQuery(cq);
				
				 list= query.getResultList();
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
					return list;
			}
		} catch (Exception e) {
			dto = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		
		return list;
	}
}
