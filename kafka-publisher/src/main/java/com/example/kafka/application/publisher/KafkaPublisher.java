package com.example.kafka.application.publisher;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.kafka.application.to.TransactionTO;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaPublisher {
	
    @Value(value = "${kafka.topic.name}")
    private String topicName;
     
    @Autowired
    private KafkaTemplate<String, TransactionTO> kafkaTemplate;

    public void sendTransactionMessage(TransactionTO transaction) {
        
    	ProducerRecord<String,TransactionTO> record=new ProducerRecord<>(topicName, transaction.getTransactionId(),transaction);
    	ListenableFuture<SendResult<String, TransactionTO>> future = this.kafkaTemplate.send(record);
        future.addCallback(new ListenableFutureCallback<SendResult<String, TransactionTO>>() {
            @Override
            public void onSuccess(SendResult<String, TransactionTO> result) {
            	RecordMetadata metadata=result.getRecordMetadata();
                log.info("Transaction record [{}], key[{}] to partition [{}], offset [{}] sent successfully",transaction,transaction.getTransactionId(),metadata.partition(), metadata.offset());
            }
 
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error while sending transaction record : " + transaction, ex);
            }
       });
    }
}
