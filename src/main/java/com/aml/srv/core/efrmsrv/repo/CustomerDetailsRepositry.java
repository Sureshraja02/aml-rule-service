package com.aml.srv.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.aml.srv.core.efrmsrv.entity.CustomerDetailsEntity;

import jakarta.data.repository.Repository;

@Repository
public interface CustomerDetailsRepositry<T> extends JpaRepository<CustomerDetailsEntity, String>  {

}