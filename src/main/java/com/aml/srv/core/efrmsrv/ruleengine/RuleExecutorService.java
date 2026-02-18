package com.aml.srv.core.efrmsrv.ruleengine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.aml.srv.core.efrmsrv.entity.NormalizedTblEntity;
import com.aml.srv.core.efrmsrv.entity.TransactionDetailsEntity;
import com.aml.srv.core.efrmsrv.utils.RuleWhizConstants;
import com.google.gson.Gson;

@EnableAsync(proxyTargetClass=true)
@EnableCaching
@Configuration
@EnableKafka
public class RuleExecutorService {

	private Logger LOGGER = LoggerFactory.getLogger(RuleExecutorService.class);
	
	@Autowired
	private RulewhizConfig appConfig;
	
	@Value("${aml.rule.thread.executor.core.pool.size:32}")
	private Integer corePoolSize;
	
	@Value("${aml.rule.thread.executor.max.pool.size:128}")
	private Integer maxPoolSize;
	
	@Value("${aml.rule.thread.executor.queue.capacity:1000}")
	private Integer queueCapacity;
	

	@Value("${aml.rule.thread.executor.keep.alive.seconds:120}")
	private Integer keepAliveSeconds;
	
	@Value("${aml.rule.thread.executor.wait.terminate.seconds:60}")
	private Integer waitTerminatSeconds;
	
	@Value("${aml.rule.thread.executor.complete.shutdown:true}")
	private boolean complete2Shutdown;
	
	List<ConcurrentMessageListenerContainer<String, String>> containers = null;
	String clazzName = RuleExecutorService.class.getSimpleName();

    @Bean(name = "RuleEngineExecutor")
    public Executor ruleEngineThrdExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);          // baseline threads
        executor.setMaxPoolSize(maxPoolSize);           // burst capacity
        executor.setQueueCapacity(queueCapacity);        // queue before spawning new threads
        executor.setKeepAliveSeconds(keepAliveSeconds);      // idle timeout
        executor.setThreadNamePrefix("RuleEngineExec-");
        executor.setWaitForTasksToCompleteOnShutdown(complete2Shutdown);
        executor.setAwaitTerminationSeconds(waitTerminatSeconds);
        executor.setAllowCoreThreadTimeOut(false);
        executor.initialize();
        return executor;
    }

    @Bean
    public List<ConcurrentMessageListenerContainer<String, String>> listenerContainers(
            ConcurrentKafkaListenerContainerFactory<String, String> factory, ProcessEventsService myService) {
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		stopListeners();
		try {
			LOGGER.info("Dynamic Kafak Lister Creation Method Called............");
			containers = new ArrayList<>();

			Integer rouleCount = appConfig.ruleEntity.size();
			LOGGER.info("Dynamic Kafak Lister Creation rouleCount [{}] : ", rouleCount);
			for (int i = 0; i < rouleCount; i++) {
				final int groupId = i; // make a final copy
				NormalizedTblEntity ruleEntity = appConfig.ruleEntity.get(i);
				ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(RuleWhizConstants.KAFKA_PUB_TOPIC_DYNAMIC);
				container.getContainerProperties().setGroupId(RuleWhizConstants.KAFKA_PUB_TOPIC_GRP + appConfig.ruleEntity.get(groupId).getId());
				container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
				container.getContainerProperties().setAsyncAcks(true);
				container.setConcurrency(3);
				container.setupMessageListener((AcknowledgingMessageListener<String, String>) (record, acknowledgment) -> {

					//LOGGER.info("Time : {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					//LOGGER.info("Group: [{}] | Message : [{}]\n",KAFKA_PUB_TOPIC_GRP + appConfig.ruleEntity.get(groupId).getId(), record.value());

					//processEvent(new Gson().fromJson(record.value(), TransactionDetailsEntity.class), KAFKA_PUB_TOPIC_GRP + appConfig.ruleEntity.get(groupId).getId(), ruleEntity);
					//ProcessEventsService myService = context.getBean(ProcessEventsService.class);
					myService.processEvent(new Gson().fromJson(record.value(), TransactionDetailsEntity.class),
							RuleWhizConstants.KAFKA_PUB_TOPIC_GRP + appConfig.ruleEntity.get(groupId).getId(), ruleEntity);
					
					if (acknowledgment != null) {
						acknowledgment.acknowledge();
						LOGGER.info("Group: [{}]  acknowledgment completed...........\n", appConfig.ruleEntity.get(groupId).getId());
					}
					try {
						// Long stTime = new Date().getTime();
						Thread.sleep(7000);
						// Long enddedTime = new Date().getTime();
						// LOGGER.info("................Thread Wait - [{}].....................\n",
						// commonUtils.findIsHourMinSec(enddedTime-stTime));
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					
				});
				container.start();
				containers.add(container);
				
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in {}@{} : {}", clazzName, methodName, e);
		} finally {
			LOGGER.info("Dynamic Kafak Lister Creation Method End............\n\n");
		}
		return containers;
	}

	public void stopListeners() {
		if (containers != null) {
			LOGGER.info("stopListeners Called...........");
			containers.forEach(ConcurrentMessageListenerContainer::stop);
		}
	}
}