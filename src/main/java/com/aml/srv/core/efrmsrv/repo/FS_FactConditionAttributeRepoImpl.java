package com.aml.srv.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.entity.FS_FactConditionAttributeEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Component
public class FS_FactConditionAttributeRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(FS_FactConditionAttributeRepoImpl.class);

	@Autowired
	EntityManager em;

	public List<FS_FactConditionAttributeEntity> getCondititonAttributes(String conditionId, String reqId) {
		LOGGER.info("REQ ID : [{}] - FS_FactConditionAttributeRepoImpl@getCondititonAttributes method called.....", reqId);
		List<FS_FactConditionAttributeEntity> condAttributLstObj = null;
		CriteriaBuilder cb = null;
		List<Predicate> predicates = null;
		CriteriaQuery<FS_FactConditionAttributeEntity> cq = null;
		Root<FS_FactConditionAttributeEntity> rootBk = null;
		TypedQuery<FS_FactConditionAttributeEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(FS_FactConditionAttributeEntity.class);
			predicates = new ArrayList<Predicate>();
			rootBk = cq.from(FS_FactConditionAttributeEntity.class);
			predicates.add(cb.equal(rootBk.get("conId"), conditionId));
			if (predicates != null) {
				cq.where(predicates.toArray(new Predicate[] {}));
				query = em.createQuery(cq);
				condAttributLstObj = query.getResultList();
			}
		} catch (Exception e) {
			condAttributLstObj = null;
			LOGGER.error("REQ ID : [{}] - Exception found in FS_FactConditionAttributeRepoImpl@getCondititonAttributes : {}", reqId, e);
		} finally {
			cb = null; predicates = null; cq = null; rootBk = null;query = null;
			LOGGER.info("REQ ID : [{}] - FS_FactConditionAttributeRepoImpl@getCondititonAttributes method end.....\n\n", reqId);
		}
		return condAttributLstObj;
	}

}
