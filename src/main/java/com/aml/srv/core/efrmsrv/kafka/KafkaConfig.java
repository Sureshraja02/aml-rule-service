package com.aml.srv.core.efrmsrv.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import com.aml.srv.core.efrmsrv.utils.AMLConstants;

/**
 * 
 * @author : E5554365 (Prabakaran.R)
 * @Project : aml-srv
 * @year : 2025
 */
@Configuration
public class KafkaConfig {
	
	/*
	 * @Value(value = "${kafka.backoff.interval:10000}") private Long interval;
	 * 
	 * @Value(value = "${kafka.backoff.max_failure:5}") private Long maxAttempts;
	 */

    /**
     * 
     * @return
     * kafkaRequestTopic
     * NewTopic
     */
    @Bean
    NewTopic kafkaRequestTopic() {
		return TopicBuilder.name(AMLConstants.KAFKA_PUB_TOPIC).partitions(3).compact().build();
	}

    /**
     * 
     * @return
     * kafkaResponseTopic
     * NewTopic
     */
    @Bean
    NewTopic kafkaResponseTopic() {
		return TopicBuilder.name(AMLConstants.KAFAK_RETURN_PUB_TOPIC).partitions(3).compact().build();
	}

}
