package com.example.kafka.application.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.application.consumer.TransactionConsumer;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping("/swagger")
@Api(value="kafkaOffsetController", description="Operations useful to seek to different offsets of the topic")
public class SwaggerController {
	
	@Autowired
	private TransactionConsumer consumer;
	
	@GetMapping("/currentlyAssignedPartitions")
	@ApiOperation("Get currently assigned partitions of the topic")
	public List<Integer> getCurrentlyAssignedPartitions() {
		return consumer.getCurrentlyAssingedPartitions();
	}
	
	@PostMapping("/seekOffsetToBeginning/{partition}")
	@ApiOperation("Seek Offset to the beginning of the topic for given partition")
	public void seekOffsetToBeginning(@PathVariable("partition") Integer partition) {
		consumer.seekOffsetToBeginning(partition);
	}
	
	@PostMapping("/seekOffsetToEnd/{partition}")
	@ApiOperation("Seek Offset to the end of the topic for given partition")
	public void seekOffsetToEnd(@PathVariable("partition") Integer partition) {
		consumer.seekOffsetToEnd(partition);
	}

	@PostMapping("/seekToAnOffset")
	@ApiOperation("Seek to an offset of the topic for given partition")
	public void seekToAnOffset(@RequestParam Integer partition,@RequestParam Long offset) {
		consumer.seekToAnOffset(partition, offset);
	}

}
