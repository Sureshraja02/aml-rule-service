package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.AccountStatusEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class AccountStatusRepositryImpl {

	private Logger LOGGER = LoggerFactory.getLogger(AccountStatusRepositryImpl.class);

	@Autowired
	EntityManager entityManager;

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
	
	
}
