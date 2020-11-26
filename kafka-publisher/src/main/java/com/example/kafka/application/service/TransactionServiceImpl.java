package com.example.kafka.application.service;

import java.time.LocalDate;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.example.kafka.application.publisher.KafkaPublisher;
import com.example.kafka.application.to.TransactionTO;

@Component
public class TransactionServiceImpl implements TransactionService{
	
	@Autowired
	private KafkaPublisher publisher;
	
	public void generateTransactionRecords() {
		
		int index=0;
		Random random = new Random(); 
		while(true) {
			TransactionTO transaction=new TransactionTO();
			transaction.setTransactionId("TRXN"+random.nextLong());
			transaction.setTransactionDate(LocalDate.now());
			transaction.setCreditDebitIndicator(index%2==0?"C":"D");
			transaction.setAccountHolderName("Some Random User");
			transaction.setAmountTransacted(100L);
			transaction.setAccountId("ABC12345");
			index++;
			
			publisher.sendTransactionMessage(transaction);
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


}
