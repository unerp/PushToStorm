package com.main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.storm.bolt.MyBolt;
import com.storm.spout.MySpout;

public class PushToStorm {
	
	public static final String topologyName = "PushToStorm"; 
	
	public static ConnectionFactory factory = null;
	public static QueueingConsumer consumer = null;
	
	public PushToStorm(){
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		LocalCluster cluster = null;
		
		builder.setSpout("MySpout",  new MySpout(), 1);
		builder.setBolt("MyBolt", new MyBolt(), 4).shuffleGrouping("MySpout");
		
		
		Config conf = new Config();
		conf.setDebug(true);
		
//		cluster = new LocalCluster();
//		cluster.submitTopology("MyTopology", conf, builder.createTopology());
		
		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		test = new Test();
		
//		Utils.sleep(100*1000);
//		cluster.killTopology("MyTopology");
//		cluster.shutdown();
		
	}

}
