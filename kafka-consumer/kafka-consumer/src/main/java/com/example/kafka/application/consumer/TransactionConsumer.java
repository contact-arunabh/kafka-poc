package com.example.kafka.application.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.stereotype.Component;

import com.example.kafka.application.service.TransactionService;
import com.example.kafka.application.to.TransactionTO;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransactionConsumer extends AbstractConsumerSeekAware{
	
	private KafkaConsumerErrorHandler errorHandler;
	private TransactionService service;
	private Long nackInterval;
	private String topic;
	
	@Autowired
	public TransactionConsumer(KafkaConsumerErrorHandler errorHandler,
							   TransactionService service,
							   @Value("${kafka.nackInterval}") Long nackInterval,
							   @Value("${kafka.transaction.topicName}") String topic) {
		this.errorHandler=errorHandler;
		this.service=service;
		this.nackInterval=nackInterval;
		this.topic=topic;
	}
	
	@KafkaListener(topics = "${kafka.transaction.topicName}", 
				   groupId = "${kafka.transaction.groupId}",
				   containerFactory = "kafkaListenerContainerFactory")
    public void listener(ConsumerRecords<String,TransactionTO> records, Acknowledgment acknowledgment) {
        boolean acked=false;
        try {
        	List<ConsumerRecord<String,TransactionTO>> validRecords=new ArrayList<>();
        	validRecords=filter(records);
        	
        	int index=0;
        	for(ConsumerRecord<String,TransactionTO> record: validRecords) {
        		try {
        			log.info("Received a message on Kafka consumer for partition-{}, offset-{},Transaction Record-{},",
							record.partition(),record.offset(),record.value());
					service.processTransaction(record.value());
					index++;
        		}catch(Exception e) {
        			log.error("Error while consuming record from partition-{}, offset-{}, record-{}",
        					record.partition(),record.offset(),record.value());
        			String errorMessage="Failed Record from partition-"+ record.partition()+", offset-"+record.offset();
        			errorHandler.handleError(errorMessage, e, record.value());
        			acknowledgment.nack(index, nackInterval);
        			acked=true;
        		}
        	}
        	
        } catch(Exception e) {
        	log.error("Error while consuming batch from partitions-{},count-{},startOffset-{}",
        			records.partitions(),records.count(),records.iterator().next().offset(), e);
        	acknowledgment.nack(nackInterval);
        	acked=true;
        } finally {
        	if(!acked) 
        		acknowledgment.acknowledge();
        }
    }
	
	private List<ConsumerRecord<String,TransactionTO>> filter(ConsumerRecords<String,TransactionTO> records) {
		List<ConsumerRecord<String,TransactionTO>> validRecords=new ArrayList<>();
		records.forEach(record -> {
			if(!checkForDeserializationException(record)) {
				validRecords.add(record);
			}
		});
		return validRecords;
	}
	
	private boolean checkForDeserializationException(ConsumerRecord<String,TransactionTO> record) {
		LogAccessor logAccessor=new LogAccessor(TransactionConsumer.class);
		DeserializationException exception=ListenerUtils.getExceptionFromHeader(record, 
				ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, logAccessor);
		if(exception!=null) {
			String errorMessage="Error while deserializing the record" +record.value()+", offset:"+record.offset();
			errorHandler.handleError(errorMessage, exception, record.value());
			return true;
		}
		return false;
	}
	
	public List<Integer> getCurrentlyAssingedPartitions() {
		return super.getSeekCallbacks().keySet().stream().map(TopicPartition::partition).collect(Collectors.toList());
	}
	
	public void seekOffsetToBeginning(int partition) {
		super.getSeekCallbackFor(new TopicPartition(topic,partition)).seekToBeginning(topic, partition);
	}
	
	public void seekOffsetToEnd(int partition) {
		super.getSeekCallbackFor(new TopicPartition(topic,partition)).seekToEnd(topic, partition);
	}
	
	public void seekToAnOffset(int partition,long offset) {
		super.getSeekCallbackFor(new TopicPartition(topic,partition)).seek(topic, partition, offset);
	}

}
