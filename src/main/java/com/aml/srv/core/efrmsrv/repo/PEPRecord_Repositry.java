package com.aml.srv.core.efrmsrv.repo;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.aml.srv.core.efrmsrv.entity.PEPRecordEntity;

import jakarta.data.repository.Repository;

@Repository
public interface PEPRecord_Repositry  extends JpaRepository<PEPRecordEntity, String>{
	
	

    @Query(value = "SELECT * FROM amlschema.FS_PEP_DATA", nativeQuery = true)
    List<PEPRecordEntity> getAllList();

}