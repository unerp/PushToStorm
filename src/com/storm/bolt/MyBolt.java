package com.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple) {
		String value = tuple.getStringByField("say");
		System.out.println("=================================================== STORM Bolt Recieve Tuple value is : " + value);
	}

	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext, OutputCollector paramOutputCollector) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer paramOutputFieldsDeclarer) {
		
	}

}
