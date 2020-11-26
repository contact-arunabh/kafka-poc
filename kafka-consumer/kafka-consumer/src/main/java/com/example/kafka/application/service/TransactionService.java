package com.example.kafka.application.service;

import com.example.kafka.application.to.TransactionTO;

public interface TransactionService {
	
	public void processTransaction(TransactionTO record) throws Exception;

}
