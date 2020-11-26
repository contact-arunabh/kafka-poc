package com.example.kafka.application.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.example.kafka.application.to.TransactionTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaConsumerErrorHandler {
	
	@Autowired
	private ObjectMapper mapper;
	
	public void handleError(String customErrorMessage, Exception e, TransactionTO failedRecord) {
		String message=null;
		try {
			message = customErrorMessage+ "\n Record:"+mapper.writeValueAsString(failedRecord) +
					"\n Stacktrace:" +getExceptionStacktrace(e);
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();
		}
		log.error(message);
	}

	private String getExceptionStacktrace(Exception ex) {
	    StringBuffer sb = new StringBuffer(500);
	    StackTraceElement[] st = ex.getStackTrace();
	    sb.append(ex.getClass().getName() + ": " + ex.getMessage() + "\n");
	    for (int i = 0; i < st.length; i++) {
	      sb.append("\t at " + st[i].toString() + "\n");
	    }
	    return sb.toString();
	}
}
