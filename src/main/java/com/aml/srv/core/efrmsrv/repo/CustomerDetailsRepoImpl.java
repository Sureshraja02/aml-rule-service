package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class CustomerDetailsRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(CustomerDetailsRepoImpl.class);

	@Autowired
	EntityManager em;

	public List<CustomerDetailsEntity> getCustomerDetailsbyCriteria(String custId) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<CustomerDetailsEntity> cq = cb.createQuery(CustomerDetailsEntity.class);

		Root<CustomerDetailsEntity> book = cq.from(CustomerDetailsEntity.class);
		List<Predicate> predicates = new ArrayList<Predicate>();
		predicates.add(cb.equal(book.get("customerId"), custId));
		// Predicate authorNamePredicate = cb.equal(book.get("acknowledgementNo"),
		// ackNo);
		// Predicate titlePredicate = cb.like(book.get("title"), "%" + title + "%");
		cq.where(predicates.toArray(new Predicate[] {}));

		TypedQuery<CustomerDetailsEntity> query = em.createQuery(cq);
		return query.getResultList();

	}

	public CustomerDetailsEntity getCustomerDetailsByCustId(String custId) {
		CriteriaBuilder cb = null;
		CriteriaQuery<CustomerDetailsEntity> cq = null;
		Root<CustomerDetailsEntity> book = null;
		List<Predicate> predicates = null;
		CustomerDetailsEntity entity = null;
		TypedQuery<CustomerDetailsEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(CustomerDetailsEntity.class);
			book = cq.from(CustomerDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			predicates.add(cb.equal(book.get("customerId"), custId));
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
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

	public String getPanStatus(String reqId, String accNo, String custId, String transMode, String transType,
			Integer hours, Integer days, Integer months, String fieldName, Range range) {
		LOGGER.info("REQID : [{}] - CustomerDetailsRepoImpl@getPanStatus method called...........", reqId);
		String retnVal = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<CustomerDetailsEntity> cq = null;

		Root<CustomerDetailsEntity> rootBk = null;
		TypedQuery<CustomerDetailsEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();

			cq = cb.createQuery(CustomerDetailsEntity.class);

			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(CustomerDetailsEntity.class);
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(rootBk.get("customerId"), custId));
			} else if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(rootBk.get("accountNo"), accNo));
			}

			LOGGER.info("REQID : [{}] - transType : [{}]", reqId, transType);

			LOGGER.info("REQID : [{}] - No of days : [{}]", reqId, days);

			LOGGER.info("REQID : [{}] - columnName is : ", reqId);
			if (predicates != null) {

				cq.where(predicates.toArray(new Predicate[] {}));

				query = em.createQuery(cq);

				CustomerDetailsEntity customerEnityObj = query.getSingleResult();

				if (customerEnityObj != null && customerEnityObj.getPanNo() != null) {

					retnVal = "AVAILABLE";
					LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, retnVal);
				} else {
					retnVal = "NO_PAN";
					LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, retnVal);
				}
			}
		} catch (Exception e) {
			retnVal = null;
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