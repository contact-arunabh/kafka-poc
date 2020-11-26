package com.example.kafka.application.to;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionTO {
	
	private String transactionId;
	private String accountId;
	private LocalDate transactionDate;
	private String accountHolderName;
	private String creditDebitIndicator;
	private Long amountTransacted;

}
