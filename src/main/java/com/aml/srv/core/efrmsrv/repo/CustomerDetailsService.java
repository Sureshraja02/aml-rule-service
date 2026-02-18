package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Service
public class CustomerDetailsService {

	private Logger LOGGER = LoggerFactory.getLogger(CustomerDetailsService.class);

	@Autowired
	EntityManager entityManager;

	

	public CustomerDetailsEntity getCustomerDetails(String reqId,String custId) {
		LOGGER.info("REQID : [{}] - CustomerDetailsService@getCustomerDetails method called...........",
				reqId);
		CriteriaBuilder cb = null;
		CriteriaQuery<CustomerDetailsEntity> cq = null;
		Root<CustomerDetailsEntity> book = null;
		List<Predicate> predicates = null;
		CustomerDetailsEntity entity = null;
		TypedQuery<CustomerDetailsEntity> query = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(CustomerDetailsEntity.class);
			book = cq.from(CustomerDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			predicates.add(cb.equal(book.get("customerId"), custId));
			cq.where(predicates.toArray(new Predicate[] {}));

			query = entityManager.createQuery(cq);
			CustomerDetailsEntity custDtls = query.getSingleResult();
			if (custDtls != null) {
				entity = custDtls;

				LOGGER.info("custId : [{}] - retnVal : [{}]", custId, custDtls);
			} else {
				entity = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", custId, custDtls);
			}
			return entity;
		} catch (Exception e) {
			return null;
		} finally {

		}
	}

	public String getCustomerDetails(String reqId,String custId, String custType) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getCustomerDetails(String reqId,String custId, String custType,String custCategory) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getCustomerDetails(String reqId,String custId, String custType,String custCategory,String nationality) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getCustomerDetails(String reqId,String custId, String custType,String custCategory,String nationality,String occupation) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getCustomerDetails(String reqId,String custId, String custType,String custCategory,String nationality,String occupation,String city) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getCustomerDetails(String reqId,String custId, String custType,String custCategory,String nationality,String occupation,String city,String state,String country) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getPanStatus(String reqId, String accNo, String custId) {
		LOGGER.info("REQID : [{}] - CustomerDetailsRepoImpl@getPanStatus method called...........", reqId);
		String retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<CustomerDetailsEntity> cq = null;

		Root<CustomerDetailsEntity> rootBk = null;
		TypedQuery<CustomerDetailsEntity> query = null;
		try {
			cb = entityManager.getCriteriaBuilder();

			cq = cb.createQuery(CustomerDetailsEntity.class);

			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(CustomerDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}

			

			LOGGER.info("REQID : [{}] - columnName is : ", reqId);
			if (predicates != null) {

				cq.where(predicates.toArray(new Predicate[] {}));

				query = entityManager.createQuery(cq);

				CustomerDetailsEntity customerEnityObj = query.getSingleResult();

				if (customerEnityObj != null && customerEnityObj.getPanNo() != null) {

					retnVal = "NON_NIL";
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = "NIL";
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = "NO_PAN";
			LOGGER.info("REQID : [{}] - Exception found in CustomerDetailsRepoImpl@getPanStatus :{}", reqId, e);
		} finally {
			cb = null;
			predicates = null;
			cq = null;
			rootBk = null;
			LOGGER.info("REQID : [{}] - CustomerDetailsRepoImpl@getPanStatus method End...........\n\n", reqId);
		}
		return retnVal;
	}
}
