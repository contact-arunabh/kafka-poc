package com.example.kafka.application;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.kafka.application.to.TransactionTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

@Configuration
@ComponentScan({"com.example.kafka.application"})
@EnableKafka
public class KafkaPublisherSpringConfig {

	@Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;
	
	@Value(value="${kafka.acksConfig}")
	private String acksConfig;
	
	@Value(value="${kafka.retriesConfig}")
	private String retriesConfig;
	
	@Value(value="${kafka.batchSizeConfig}")
	private String batchSizeConfig;
	
	@Value(value="${kafka.lingerMsConfig}")
	private String lingerMsConfig;
	
	@Value(value="${kafka.bufferMemoryConfig}")
	private String bufferMemoryConfig;
	
	@Value(value="${kafka.maxInFlightRequestsPerConnection}")
	private String maxInFlightRequestsPerConnection;
	
	@Value(value="${kafka.enableIdempotence}")
	private String enableIdempotence;
	
	@Value(value="${kafka.maxBlockMsConfig}")
	private String maxBlockMsConfig;
	
	@Value(value="${kafka.requestTimeOutMs}")
	private String requestTimeOutMs;
    
    @Bean
    public ProducerFactory<String, TransactionTO> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeConfig);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMsConfig);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemoryConfig);
        configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        configProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeOutMs);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(), new JsonSerializer<TransactionTO>(objectMapper()));
    }
    
    @Bean
    public KafkaTemplate<String, TransactionTO> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }
     
	
}
