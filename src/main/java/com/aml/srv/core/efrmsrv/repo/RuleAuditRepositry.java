package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.RuleAuditReqEntity;

import jakarta.data.repository.Repository;

@Repository
public interface RuleAuditRepositry<T> extends JpaRepository<RuleAuditReqEntity, String>  {

}