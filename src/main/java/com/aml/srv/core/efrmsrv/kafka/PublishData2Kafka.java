package com.aml.srv.core.efrmsrv.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.aml.srv.core.efrmsrv.kafka.repo.FinSecIndicatorVO;
import com.google.gson.Gson;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Component
public class PublishData2Kafka {

	private static Logger LOGGER = LoggerFactory.getLogger(PublishData2Kafka.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	/**
	 * 
	 * @param keyid
	 * @param dataDetails
	 * @param resultopic
	 * sendtoKafka
	 * void
	 */
	public void sendtoKafka(String keyid, FinSecIndicatorVO dataDetails, String resultopic) {
		ProducerRecord<String, String> record = null;
		try {
			/**
			 * Parameters: topic The topic the record will be appended to partition The
			 * partition to which the record should be sent key The key that will be
			 * included in the record value The record contents
			 */
			LOGGER.info(":::::::::::::::::::::::::::::::FINSECINDICATOR : [{}]",new Gson().toJson(dataDetails));
			record = new ProducerRecord<String, String>(resultopic, null, keyid, new Gson().toJson(dataDetails));
			kafkaTemplate.send(record);
			LOGGER.info("AML DATA File processing completed. FINSEC Object pushed into KAFKA.....\n\n");
		} catch (Exception e) {
			LOGGER.error("Exception found in FileService@sendtoKafka : {}", e);
		} finally {
		}

	}
}
