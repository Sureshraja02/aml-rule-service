package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.srv.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;
import com.aml.srv.core.efrmsrv.rule.process.request.Factset;
import com.aml.srv.core.efrmsrv.rule.process.request.Range;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Service
public class AccountDetailsService{

	private Logger LOGGER = LoggerFactory.getLogger(AccountDetailsService.class);

	@Autowired
	EntityManager entityManager;

	
	public String getAccountDetails(String reqId, String custId) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getAccountOpeningAndClosingDateByritiria(String reqId, String accNo, String custId) {
		LOGGER.info("REQID : [{}] - AccountDetailsService@getAccountOpeningAndClosingDateByritiria method called...........",
				reqId);
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountDetailsEntity> cq = null;
		Root<AccountDetailsEntity> book = null;
		List<Predicate> predicates = null;
		String openingDate=null;
		String closingDate=null;
		String combinedStr=null;
		TypedQuery<AccountDetailsEntity> query = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(AccountDetailsEntity.class);
			book = cq.from(AccountDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(book.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(book.get("customerId"), custId));
			}
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = entityManager.createQuery(cq);
			AccountDetailsEntity customerEnityObj = query.getSingleResult();
			if (customerEnityObj != null && customerEnityObj.getAccountOpenedDate() != null) {
				openingDate = customerEnityObj.getAccountOpenedDate();
				if (customerEnityObj != null && customerEnityObj.getAccountClosedDate() != null) {
					
					combinedStr=openingDate+"@"+customerEnityObj.getAccountClosedDate();
				}
				else
				{
					combinedStr=openingDate;
				}
				
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, combinedStr);
			} else {
				combinedStr = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, combinedStr);
			}
		} catch (Exception e) {
			return null;
		} finally {

		}
		return combinedStr;
	}
	
	public AccountStatusEntity getAccountStatusByAccNO(String accNo, String reqId) {
		LOGGER.info("REQ ID : [{}] - AccountStatusRepositryImpl@getAccountStatusByAccNO method called.......", reqId);
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountStatusEntity> cq = null;
		Root<AccountStatusEntity> book = null;
		List<Predicate> predicates = null;
		TypedQuery<AccountStatusEntity> query = null;
		AccountStatusEntity accountStatusEntity =null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(AccountStatusEntity.class);
			book = cq.from(AccountStatusEntity.class);
			predicates = new ArrayList<Predicate>();
			predicates.add(cb.equal(book.get("accountNo"), accNo));
			cq.where(predicates.toArray(new Predicate[] {}));

			query = entityManager.createQuery(cq);
			try {
				accountStatusEntity = query.getSingleResult();
			   if(accountStatusEntity!=null) {
				   return accountStatusEntity;
			   } else {
				   return accountStatusEntity;
			   }
			} catch (NoResultException e) {
				return accountStatusEntity;
			}
			
		} catch (Exception e) {
			LOGGER.error("REQ ID : [{}] - Exception found in AccountStatusRepositryImpl@getAccountStatusByAccNO : {}", reqId, e);
			return null;
		} finally {
			 cb = null;cq = null;book = null;predicates = null;
			LOGGER.info("REQ ID : [{}] - AccountStatusRepositryImpl@getAccountStatusByAccNO method End.......", reqId);

		}
	}
	public AccountDetailsEntity getAccountDetails(String reqId,String custId, String acctNo) {
		LOGGER.info("REQ ID : [{}] - AccountStatusRepositryImpl@getAccountDetails method called.......", reqId);
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountDetailsEntity> cq = null;
		Root<AccountDetailsEntity> book = null;
		List<Predicate> predicates = null;
		TypedQuery<AccountDetailsEntity> query = null;
		AccountDetailsEntity accountDetailsEntity =null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(AccountDetailsEntity.class);
			book = cq.from(AccountDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			predicates.add(cb.equal(book.get("accountNo"), acctNo));
			cq.where(predicates.toArray(new Predicate[] {}));

			query = entityManager.createQuery(cq);
			try {
				accountDetailsEntity = query.getSingleResult();
			   if(accountDetailsEntity!=null) {
				   return accountDetailsEntity;
			   } else {
				   return accountDetailsEntity;
			   }
			} catch (NoResultException e) {
				return accountDetailsEntity;
			}
			
		} catch (Exception e) {
			LOGGER.error("REQ ID : [{}] - Exception found in AccountStatusRepositryImpl@getAccountDetails : {}", reqId, e);
			return null;
		} finally {
			 cb = null;cq = null;book = null;predicates = null;
			LOGGER.info("REQ ID : [{}] - AccountStatusRepositryImpl@getAccountDetails method End.......", reqId);

		}
	}
	public String getAccountDetails(String reqId,String custId, String acctNo,String acctType) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}
	public String getAccountDetails(String reqId,String custId, String acctNo,String acctType,String acctStatus) {
		LOGGER.info("REQID : [{}] - TransactionDetailsRepositryImpl@ruleOfImmediateWithdraw method called...........",
				reqId);
		return null;
	}

}
