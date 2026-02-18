package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.NonCustomerDetailsEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class NonCustomerDetailsRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(NonCustomerDetailsRepoImpl.class);

	@Autowired
	EntityManager em;

	public List<NonCustomerDetailsEntity> getNonCustomerDetails(String custId) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<NonCustomerDetailsEntity> cq = cb.createQuery(NonCustomerDetailsEntity.class);

		Root<NonCustomerDetailsEntity> book = cq.from(NonCustomerDetailsEntity.class);
		List<Predicate> predicates = new ArrayList<Predicate>();
		predicates.add(cb.equal(book.get("counterAccountNo"), custId));
		// Predicate authorNamePredicate = cb.equal(book.get("acknowledgementNo"),
		// ackNo);
		// Predicate titlePredicate = cb.like(book.get("title"), "%" + title + "%");
		cq.where(predicates.toArray(new Predicate[] {}));

		TypedQuery<NonCustomerDetailsEntity> query = em.createQuery(cq);
		return query.getResultList();

	}

	
	
	
}