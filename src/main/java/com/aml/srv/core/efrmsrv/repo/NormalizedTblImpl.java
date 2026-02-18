package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.srv.core.efrmsrv.entity.NormalizedTblEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class NormalizedTblImpl {

	private Logger LOGGER = LoggerFactory.getLogger(NormalizedTblImpl.class);

	@Autowired
	EntityManager entityManager;

	public List<NormalizedTblEntity> getActiveRules() {
		List<NormalizedTblEntity> normalizedTblListOb = null;

		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<NormalizedTblEntity> cq = null;
		Root<NormalizedTblEntity> rootBk = null;
		try {
			cb = entityManager.getCriteriaBuilder();
			cq = cb.createQuery(NormalizedTblEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(NormalizedTblEntity.class);
			predicates.add(cb.equal(rootBk.get("status"), 1));
			cq.where(predicates.toArray(new Predicate[] {}));
			normalizedTblListOb = entityManager.createQuery(cq).getResultList();

		} catch (Exception e) {
			LOGGER.info("Exception found in NormalizedTblImpl@getActiveRules : {}", e);
		} finally {
			 cb = null; predicates = null; cq = null; rootBk = null;
		}
		return normalizedTblListOb;
	}
}
