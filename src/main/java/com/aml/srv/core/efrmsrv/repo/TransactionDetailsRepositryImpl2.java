package com.aml.srv.core.efrmsrv.repo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;
import com.aml.srv.core.efrmsrv.rule.process.response.ComputedFactsVO;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Expression;
import jakarta.persistence.criteria.Path;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Repository
public class TransactionDetailsRepositryImpl2 {

	private Logger LOGGER = LoggerFactory.getLogger(TransactionDetailsRepositryImpl2.class);

	@Autowired
	EntityManager entityManager;

	@Autowired
	AccountDetailsRepoImpl accountDetailsRepoImpl;

	public BigDecimal getImmediateWithdraw(String reqId, String accNo, String custId, String transMode,
			String transType, Integer days, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {

				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));

				// Calculate date range in Java
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				// String todayStr = today.toString(); // yyyy-MM-dd//String startDateStr =
				// startDate.toString();
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(stDate),
						java.sql.Date.valueOf(currentDateTdy));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.max(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					retnVal = (BigDecimal) result[1];
					// retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw :{}",
					reqId, e);
		} finally {
			cb = null; 	predicates = null; cq = null; rootBk = null;
			LOGGER.info( "REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumCreditDebitAmount(String reqId, String accNo, String custId, String transMode,
			String transType, Integer days, String fieldName, Integer months, Range range, Factset factSetObj) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfSumCreditAmount method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}

			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

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

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			LocalDate currentDateTdy = null;
			LocalDate openDate = null;
			LocalDate closeDate = null;
			if (days != null) {

				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));

				if (factSetObj.getCondition() != null && factSetObj.getCondition().equals("NEW_ACCOUNT_CLOSED")) {
					String openingDate = accountDetailsRepoImpl.getAccountOpeningAndClosingDateByritiria(reqId, accNo,
							custId);
					if (openingDate != null) {
						if (openingDate.contains("@")) {
							String dateStr[] = openingDate.split("@");
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
							openDate = LocalDate.parse(dateStr[0], formatter);
							closeDate = LocalDate.parse(dateStr[1], formatter);
							System.out.println(openDate); // Output: 2025-05-20

							long daysBetween = ChronoUnit.DAYS.between(openDate, closeDate);
							if (daysBetween <= days) {
								// Account Closed Immediately
								currentDateTdy = closeDate;
							} else {
								return new BigDecimal(0);
							}
						} else {

							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
							openDate = LocalDate.parse(openingDate, formatter);
							System.out.println(openDate); // Output: 2025-05-20

							currentDateTdy = openDate.plusDays(days);
						}
					} else {
						return new BigDecimal(0);
					}
				}

				else {
					currentDateTdy = LocalDate.now();
					openDate = currentDateTdy.minusDays(days);
					System.out.println(openDate); // Output: 2025-05-20
				}

				// Calculate date range in Java

				// Convert LocalDate to String in same format as DB
				// String todayStr = today.toString(); // yyyy-MM-dd//String startDateStr =
				// startDate.toString();
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(openDate),
						java.sql.Date.valueOf(currentDateTdy));
				predicates.add(betweenDates);
			}
			if (months != null) {

				if (factSetObj.getCondition() != null && factSetObj.getCondition().equals("NEW_ACCOUNT_CLOSED")) {
					String openingDate = accountDetailsRepoImpl.getAccountOpeningAndClosingDateByritiria(reqId, accNo,
							custId);
					if (openingDate != null) {
						if (openingDate.contains("@")) {
							String dateStr[] = openingDate.split("@");
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
							openDate = LocalDate.parse(dateStr[0], formatter);
							closeDate = LocalDate.parse(dateStr[1], formatter);
							System.out.println(openDate); // Output: 2025-05-20

							long daysBetween = ChronoUnit.DAYS.between(openDate, closeDate);
							int totaldays = months * 30;
							if (daysBetween <= totaldays) {
								// Account Closed Immediately
								currentDateTdy = closeDate;
							} else {
								return new BigDecimal(0);
							}
						} else {
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
							openDate = LocalDate.parse(openingDate, formatter);
							System.out.println(openDate); // Output: 2025-05-20

							currentDateTdy = openDate.plusMonths(months);
						}
					} else {
						return new BigDecimal(0);
					}
				}

				else {
					currentDateTdy = LocalDate.now();
					openDate = currentDateTdy.minusMonths(months);
					System.out.println(openDate); // Output: 2025-05-20
				}

				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = openDate.toString();
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@ruleOfSumCreditAmount :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfSumCreditAmount method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getAvgCreditDebit(String reqId, String accNo, String custId, String transMode, String transType,
			Integer days, String fieldName, Integer months, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}

			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

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

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));

				// Calculate date range in Java
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				// String todayStr = today.toString(); // yyyy-MM-dd//String startDateStr =
				// startDate.toString();
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(stDate),
						java.sql.Date.valueOf(currentDateTdy));
				predicates.add(betweenDates);
			}
			if (months != null) {
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusMonths(months);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.avg(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Double value = (Double) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getCountValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
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
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getMaxValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMaxValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
			}
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = stDate.toString();
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.max(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getMaxValue :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMaxValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getMinBalanceValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMinValue method called...........", reqId);
		BigDecimal retnVal = new BigDecimal(0);
		try {
			AccountDetailsEntity acctDtls = accountDetailsRepoImpl.getAccountDetailsByritiria(reqId, accNo, custId);
			if (acctDtls != null) {
				if (acctDtls.getAvailableBalance() != null) {
					retnVal = BigDecimal.valueOf(acctDtls.getAvailableBalance());
				}

			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getMinValue :{}", reqId, e);
		} finally {

			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMinValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getAvgValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getAvgValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Double value = (Double) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getAvgValue :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getAvgValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getCountCreditDebit(String reqId, String accNo, String custId, String transMode, String transType,
			Integer days, String fieldName, Integer months, Range range, Factset factSetObj) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit method called...........",
				reqId);
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

			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);
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

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);
			LocalDate currentDateTdy = null;
			LocalDate openDate = null;
			LocalDate closeDate = null;
			if (days != null) {
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));

				if (factSetObj.getCondition() != null && factSetObj.getCondition().equals("NEW_ACCOUNT")) {
					String openingDate = accountDetailsRepoImpl.getAccountOpeningDateByritiria(reqId, accNo, custId);
					if (openingDate != null) {

						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
						openDate = LocalDate.parse(openingDate, formatter);
						System.out.println(openDate); // Output: 2025-05-20

						currentDateTdy = openDate.plusDays(days);

					} else {
						return new BigDecimal(0);
					}
				}

				else {
					currentDateTdy = LocalDate.now();
					openDate = currentDateTdy.minusDays(days);
					System.out.println(openDate); // Output: 2025-05-20
				}

				// Convert LocalDate to String in same format as DB
				// String todayStr = today.toString(); // yyyy-MM-dd//String startDateStr =
				// startDate.toString();
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(openDate),
						java.sql.Date.valueOf(currentDateTdy));
				predicates.add(betweenDates);
			}
			if (months != null) {
				if (factSetObj.getCondition() != null && factSetObj.getCondition().equals("NEW_ACCOUNT")) {
					String openingDate = accountDetailsRepoImpl.getAccountOpeningDateByritiria(reqId, accNo, custId);
					if (openingDate != null) {

						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
						openDate = LocalDate.parse(openingDate, formatter);
						System.out.println(openDate); // Output: 2025-05-20

						currentDateTdy = openDate.plusMonths(months);

					} else {
						return new BigDecimal(0);
					}
				}

				else {
					currentDateTdy = LocalDate.now();
					openDate = currentDateTdy.minusMonths(months);
					System.out.println(openDate); // Output: 2025-05-20
				}

				// Convert LocalDate to String in same format as DB
				String todayStr = currentDateTdy.toString(); // yyyy-MM-dd
				String startDateStr = openDate.toString();
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] -  is :", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfAvgCreditDebit method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getCountCashDepositValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
			}
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);
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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public ComputedFactsVO getSumCashDepositValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCashDepositValue method called...........",
				reqId);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		ComputedFactsVO computedFactsVO = null;
		try {
			computedFactsVO = new ComputedFactsVO();
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
			Expression<Double> maxOf = cb.max(rootBk.get("amount"));
			Expression<Long> countOf = cb.count(rootBk);
			Expression<String> maxDateExp = cb.greatest(rootBk.<String>get("transactionDate"));

			Path<String> transId = rootBk.get("transactionId");
			Path<String> counterPartyaddress = rootBk.get("counterpartyAddress");
			LOGGER.info("REQID : [{}] - transactionId  is : [{}] - counterPartyaddress : [{}]", reqId, transId,
					counterPartyaddress);

			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				Object[] result = null;
				try {
					cq.multiselect(countOf, maxOf, maxDateExp, transId, counterPartyaddress);
					result = entityManager.createQuery(cq).getSingleResult();
				} catch (Exception e) {
				}

				if (result != null && result.length > 1) {
					retnVal = (BigDecimal) result[1];
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
					LOGGER.info("REQID : [{}] - TransDate : [{}]", reqId, result[2]);
					computedFactsVO.setValue(retnVal);
					computedFactsVO.setTransDate((String) result[2]);
					computedFactsVO.setTransactionId((String) result[3]);
					computedFactsVO.setDepositeLocation((String) result[4]);
				} else {
					retnVal = new BigDecimal(0);
					computedFactsVO.setValue(retnVal);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			computedFactsVO.setValue(retnVal);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumCashDepositValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCashDepositValue method End...........\n\n",
					reqId);
		}
		return computedFactsVO;
	}

	public BigDecimal getSumNonCashDepositValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumNonCashDepositValue method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "D"));

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (cb.not(rootBk.get("channelType").in(channeltype)));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info(
					"REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumNonCashDepositValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getSumNonCashDepositValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getAvgCashDepositValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getAvgCashDepositValue method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Double value = (Double) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getAvgCashDepositValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getAvgCashDepositValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumCashWithdrawValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);

			predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "W"));

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
			predicates.add(inchanneltype);
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

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getCountCashWithdrawValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}

			predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "W"));
			List<String> type = Arrays.asList("ATM", "CASH");
			Predicate inClause = rootBk.get("channelType").in(type);
			predicates.add(inClause);

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

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getAugCashWithdrawValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "W"));

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = ((rootBk.get("channelType").in(channeltype)));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.avg(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Double value = (Double) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumNonCashWithdrawValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (cb.not(rootBk.get("channelType").in(channeltype)));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getCountAccountTransferValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info(
				"REQID : [{}] - TransactionDetailsRepositryImpl@getCountAccountTransferValue method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (cb.not(rootBk.get("channelType").in(channeltype)));
			predicates.add(inchanneltype);

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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info(
					"REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountAccountTransferValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getCountAccountTransferValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumCashTxnValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCashTxnValue method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumCashTxnValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCashTxnValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumNonCashTxnValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = cb.not(rootBk.get("channelType").in(channeltype));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumNonCashTxnValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getSumNonCashTxnValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumAccountToAccountTxn(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumAccountToAccountTxn method called...........",
				reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
			}
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info(
					"REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumAccountToAccountTxn :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getSumAccountToAccountTxn method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getSumAccountTxn(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumAccountTxn method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);

			List<String> type = Arrays.asList("W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (cb.not(rootBk.get("channelType").in(channeltype)));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumAccountTxn :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumAccountTxn method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public ComputedFactsVO getLargerDeposite(String reqId, String accNo, String custId, String transMode,
			String transType, Integer days, String fieldName, String columnName, Range rangeObj) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method called...........", reqId);
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		ComputedFactsVO computedFactsVO = null;
		try {
			computedFactsVO = new ComputedFactsVO();
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}

			predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), "D"));

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD") // format of stored string
				);

				// Calculate date range in Java
				LocalDate currentDateTdy = LocalDate.now();
				LocalDate stDate = currentDateTdy.minusDays(days);
				// Convert LocalDate to String in same format as DB
				// String todayStr = today.toString(); // yyyy-MM-dd//String startDateStr =
				// startDate.toString();
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(stDate),
						java.sql.Date.valueOf(currentDateTdy));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {

				Expression<Double> maxOf = cb.max(rootBk.get("amount"));
				Expression<Long> countOf = cb.count(rootBk);
				Expression<String> maxDateExp = cb.greatest(rootBk.<String>get("transactionDate"));
				// Expression<String> addressExp = rootBk.get("counterCountryCode");

				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(countOf, maxOf, maxDateExp);

				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					retnVal = (BigDecimal) result[1];
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
					LOGGER.info("REQID : [{}] - TransDate : [{}]", reqId, result[2]);
					computedFactsVO.setValue(retnVal);
					computedFactsVO.setTransDate((String) result[2]);
				} else {
					retnVal = new BigDecimal(0);
					computedFactsVO.setValue(retnVal);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			computedFactsVO.setValue(retnVal);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumValue :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return computedFactsVO;
	}

	public BigDecimal getMaxNonCashTxnValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, Range range) {
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = cb.not(rootBk.get("channelType").in(channeltype));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] -  is : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumNonCashTxnValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getSumNonCashTxnValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getMaxCashTxnValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, Range range) {
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			List<String> channeltype = Arrays.asList("ATM", "CASH");
			Predicate inchanneltype = (rootBk.get("channelType").in(channeltype));
			predicates.add(inchanneltype);
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - is : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					BigDecimal value = (BigDecimal) result[1];
					retnVal = value;
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumNonCashTxnValue :{}",
					reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info(
					"REQID : [{}] - TransactionDetailsRepositryImpl@getSumNonCashTxnValue method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	/*
	 * public BigDecimal getSumCreditDebitClosedAccontAmount(String reqId, String
	 * accNo, String custId, String transMode, String transType, Integer days,
	 * String fieldName,Integer months, String columnName,Range range) { LOGGER.
	 * info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCreditDebitClosedAccontAmount method called..........."
	 * , reqId); BigDecimal retnVal = null; CriteriaBuilder cb = null;
	 * List<Predicate> predicates = null; CriteriaQuery<Object[]> cq = null;
	 * Root<TransactionDetailsEntity> rootBk = null; try { cb =
	 * entityManager.getCriteriaBuilder(); cq = cb.createQuery(Object[].class);
	 * predicates = new ArrayList<Predicate>(); rootBk =
	 * cq.from(TransactionDetailsEntity.class); if (StringUtils.isNotBlank(accNo)) {
	 * predicates.add(cb.equal(rootBk.get("accountNo"), accNo)); } if
	 * (StringUtils.isNotBlank(custId)) {
	 * predicates.add(cb.equal(rootBk.get("customerId"), custId)); } String
	 * openingDate=accountDetailsRepoImpl.getAccountOpeningAndClosingDateByritiria(
	 * reqId, accNo, custId);
	 * 
	 * List<String> type = Arrays.asList("D","W"); Predicate inClause =
	 * rootBk.get("depositorWithdrawal").in(type); predicates.add(inClause);
	 * 
	 * if (range != null) { if (range.getMin() != null && range.getMax() != null) {
	 * predicates.add(cb.between(rootBk.get("amount"), range.getMin(),
	 * range.getMax())); } else if (range.getMin() != null) { // Only min present →
	 * greaterThanOrEqualTo
	 * predicates.add(cb.greaterThanOrEqualTo(rootBk.get("amount"),
	 * range.getMin())); } else if (range.getMax() != null) { // Only max present →
	 * lessThanOrEqualTo predicates.add(cb.lessThanOrEqualTo(rootBk.get("amount"),
	 * range.getMax())); }
	 * 
	 * }
	 * 
	 * LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);
	 * 
	 * if (days != null) { Expression<java.sql.Date> txnDateAsDate =
	 * cb.function("to_Date", java.sql.Date.class, rootBk.get("transactionDate"),
	 * cb.literal("YYYY-MM-DD"));
	 * 
	 * // Calculate date range in Java DateTimeFormatter formatter =
	 * DateTimeFormatter.ofPattern("dd/MM/yyyy"); LocalDate date =
	 * LocalDate.parse(openingDate, formatter); System.out.println(date); // Output:
	 * 2025-05-20
	 * 
	 * LocalDate stDate = date.plusDays(days); // Convert LocalDate to String in
	 * same format as DB // String todayStr = today.toString(); //
	 * yyyy-MM-dd//String startDateStr = // startDate.toString(); Predicate
	 * betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(date),
	 * java.sql.Date.valueOf(stDate)); predicates.add(betweenDates); } if (months !=
	 * null) {
	 * 
	 * // Calculate date range in Java DateTimeFormatter formatter =
	 * DateTimeFormatter.ofPattern("dd/MM/yyyy"); LocalDate date =
	 * LocalDate.parse(openingDate, formatter); System.out.println(date); // Output:
	 * 2025-05-20
	 * 
	 * 
	 * 
	 * LocalDate stDate = date.plusMonths(months); // Convert LocalDate to String in
	 * same format as DB
	 * 
	 * String startDateStr = stDate.toString();
	 * //LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate); //
	 * LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId,
	 * String.valueOf(stDate)); //
	 * LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId,
	 * String.valueOf(currentDateTdy));
	 * LOGGER.info("REQID : [{}] - Account Opeing Date : [{}]  afterDays : [{}]",
	 * reqId, date,startDateStr); Expression<java.sql.Date> txnDateAsDate =
	 * cb.function("to_Date", java.sql.Date.class, rootBk.get("transactionDate"),
	 * cb.literal("YYYY-MM-DD")); Predicate betweenDates = cb.between(txnDateAsDate,
	 * java.sql.Date.valueOf(date), java.sql.Date.valueOf(stDate));
	 * predicates.add(betweenDates); }
	 * LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName); if
	 * (predicates != null && StringUtils.isNotBlank(columnName) &&
	 * columnName.equalsIgnoreCase("amount")) {
	 * cq.where(cb.and(predicates.toArray(new Predicate[0])));
	 * cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount"))); Object[]
	 * result = entityManager.createQuery(cq).getSingleResult(); if (result != null
	 * && result.length > 1) { BigDecimal value= (BigDecimal) result[1];
	 * retnVal=value; LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
	 * } else {
	 * LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId,
	 * retnVal); } } } catch (Exception e) { retnVal = new BigDecimal(0); LOGGER.
	 * info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getSumCreditDebitClosedAccontAmount :{}"
	 * , reqId, e); } finally { cb = null; predicates = null; cq = null; rootBk =
	 * null; LOGGER.
	 * info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumCreditDebitClosedAccontAmount method End...........\n\n"
	 * , reqId); } return retnVal; }
	 */

	public String getAccountStatus(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range,
			Factset factSetObj) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getAccountStatus method called...........", reqId);
		String retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<Object[]> cq = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(Object[].class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			String status = accountDetailsRepoImpl.getAccountStatus(reqId, accNo, custId, transMode, transType, hours,
					days, months, fieldName, columnName, range);

			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (status != null) {

				retnVal = status;
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
			} else {
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);

			}
		} catch (Exception e) {
			retnVal = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getAccountStatus :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getAccountStatus method End...........\n\n",
					reqId);
		}
		return retnVal;
	}

	public BigDecimal getMinValue(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMinValue method called...........", reqId);
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
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
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
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			if (days != null) {
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.min(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getMinValue :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getMinValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public BigDecimal getTxnAmount(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, String columnName, Range range,
			String txnId) {
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<TransactionDetailsEntity> cq = null;
		TypedQuery<TransactionDetailsEntity> query = null;
		TransactionDetailsEntity txnDetails = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(TransactionDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null && StringUtils.isNotBlank(columnName) && columnName.equalsIgnoreCase("amount")) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				// cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				// Object[] result = entityManager.createQuery(cq).getSingleResult();
				query = entityManager.createQuery(cq);
				try {
					txnDetails = query.getSingleResult();
					if (txnDetails != null) {
						LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, txnDetails.getAmount());
						return txnDetails.getAmount();

					} else {
						LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, 0);
						return new BigDecimal(0);
					}
				} catch (NoResultException e) {
					return new BigDecimal(0);
				}

			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getTxnAmount :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getTxnAmount method End...........\n\n", reqId);
		}
		return retnVal;
	}

	public TransactionDetailsEntity getTxnDetails(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, String columnName,
			Range range, String txnId) {
		BigDecimal retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<TransactionDetailsEntity> cq = null;
		TypedQuery<TransactionDetailsEntity> query = null;
		TransactionDetailsEntity txnDetails = null;
		Root<TransactionDetailsEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(TransactionDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(TransactionDetailsEntity.class);
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			}
			if (StringUtils.isNotBlank(txnId)) {
				predicates.add(cb.equal(rootBk.get("transactionId"), txnId));
			}

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			List<String> type = Arrays.asList("D", "W");
			Predicate inClause = rootBk.get("depositorWithdrawal").in(type);
			predicates.add(inClause);

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, columnName);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				// cq.multiselect(cb.count(rootBk), cb.sum(rootBk.get("amount")));
				// Object[] result = entityManager.createQuery(cq).getSingleResult();
				query = entityManager.createQuery(cq);
				try {
					txnDetails = query.getSingleResult();
					if (txnDetails != null) {
						LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, txnDetails);
						return txnDetails;

					} else {
						LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, 0);
						return null;
					}
				} catch (NoResultException e) {
					return null;
				}

			}
		} catch (Exception e) {
			txnDetails = null;
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getTxnAmount :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getTxnAmount method End...........\n\n", reqId);
		}
		return txnDetails;
	}

	public BigDecimal getCountSmallCashDepositValue(String reqId, String accNo, String custId, String transMode,
			String transType, Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getCountValue method called...........", reqId);
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

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);
			if (StringUtils.isNotBlank(transType)) {
				predicates.add(cb.equal(rootBk.get("depositorWithdrawal"), transType));
			}
			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);
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
				// Expression<java.sql.Date> txnDateAsDate = cb.function("TRANS_DATE",
				// java.sql.Date.class, rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD")
				// // format of stored string);

				/*
				 * Expression<java.sql.Date> txnDateAsDate = cb.function("TO_DATE",
				 * java.sql.Date.class, rootBk.get("transactionDate"),
				 * cb.literal("YYYY-MM-DD"));
				 */
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
				// LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, stDate);
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(stDate));
//				LOGGER.info("REQID : [{}] - columnName is : [{}]", reqId, String.valueOf(currentDateTdy));
				LOGGER.info("REQID : [{}] - Current / today Date : [{}]  startDateStr : [{}]", reqId, todayStr,
						startDateStr);
				Expression<java.sql.Date> txnDateAsDate = cb.function("to_Date", java.sql.Date.class,
						rootBk.get("transactionDate"), cb.literal("YYYY-MM-DD"));
				Predicate betweenDates = cb.between(txnDateAsDate, java.sql.Date.valueOf(startDateStr),
						java.sql.Date.valueOf(todayStr));
				predicates.add(betweenDates);
			}
			LOGGER.info("REQID : [{}] - : [{}]", reqId);
			if (predicates != null) {
				cq.where(cb.and(predicates.toArray(new Predicate[0])));
				cq.multiselect(cb.count(rootBk), cb.count(rootBk.get("amount")));
				Object[] result = entityManager.createQuery(cq).getSingleResult();
				if (result != null && result.length > 1) {
					Long value = (Long) result[1];
					retnVal = BigDecimal.valueOf(value);
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = new BigDecimal(0);
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = new BigDecimal(0);
			LOGGER.info("REQID : [{}] - Exception found in TransactionDetailsRepositryImpl@getCountValue :{}", reqId,
					e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@getSumValue method End...........\n\n", reqId);
		}
		return retnVal;
	}

}
