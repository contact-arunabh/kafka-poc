package com.example.kafka.application.service;

import org.springframework.stereotype.Component;

import com.example.kafka.application.to.TransactionTO;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TransactionServiceImpl implements TransactionService{
	
	public void processTransaction(TransactionTO record) throws Exception{
		log.info("Processed transaction record with details-{}", record);
	}

}
