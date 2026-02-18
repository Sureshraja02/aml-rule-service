package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.FactSetMasterEntity;

import jakarta.data.repository.Repository;

@Repository
public interface AGG_Group_MasterRepositry<T> extends JpaRepository<FactSetMasterEntity, String> {

}