package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.FinsentinelRiskEntity;

import jakarta.data.repository.Repository;

@Repository
public interface FinsentinelRiskRepositry<T> extends JpaRepository<FinsentinelRiskEntity, Integer> {

}
