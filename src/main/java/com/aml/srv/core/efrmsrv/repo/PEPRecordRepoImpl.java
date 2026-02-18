package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.PEPRecordEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class PEPRecordRepoImpl {
	
	@Autowired
	EntityManager em;

	public List<PEPRecordEntity> getCustomerDetailsbyCriteria(String custId) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<PEPRecordEntity> cq = cb.createQuery(PEPRecordEntity.class);

		Root<PEPRecordEntity> book = cq.from(PEPRecordEntity.class);
		List<Predicate> predicates = new ArrayList<Predicate>();
		predicates.add(cb.equal(book.get("customerId"), custId));
		// Predicate authorNamePredicate = cb.equal(book.get("acknowledgementNo"),
		// ackNo);
		// Predicate titlePredicate = cb.like(book.get("title"), "%" + title + "%");
		cq.where(predicates.toArray(new Predicate[] {}));

		TypedQuery<PEPRecordEntity> query = em.createQuery(cq);
		return query.getResultList();

	}
	
	
}