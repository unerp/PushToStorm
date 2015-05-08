package com.storm.spout;

import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;

import java.net.MalformedURLException;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import sun.awt.SunHints.Value;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

//import com.github.nkzawa.socketio.client.Socket;

public class MySpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private static final String EXCHANGE_NAME = "storm.client.push";
	
	private static SpoutOutputCollector collector;
	private static long count = 0;
	
	private SocketIO socket = null;
	
	public void nextTuple() {
//		this.collector.emit(new Values("Hello World!!!"));
		
//		try {
//			Thread.sleep(1*1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		this.emit();
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		
		try {
			
			socket = new SocketIO("http://172.16.160.133:8686");
			socket.connect(new IOCallback() {
				@Override
				public void onMessage(JSONObject json, IOAcknowledge ack) {
					try {
						System.out.println("Server said:" + json.toString(2));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}

				@Override
				public void onMessage(String data, IOAcknowledge ack) {
					System.out.println("Server said: " + data);
				}

				@Override
				public void onError(SocketIOException socketIOException) {
					System.out.println("an Error occured");
					socketIOException.printStackTrace();
				}

				@Override
				public void onDisconnect() {
					System.out.println("Connection terminated.");
				}

				@Override
				public void onConnect() {
					System.out.println("Connection established");
				}

				@Override
				public void on(String event, IOAcknowledge ack, Object... args) {
					if(event.equals(EXCHANGE_NAME)){
						if(args.length > 0){
							JSONObject json = (JSONObject)args[0];
							try {
								System.out.println("Storm Spout Receive Data : " + json.getString("msg"));
								MySpout.collector.emit(new Values(json.getString("msg")));
							} catch (JSONException e) {
								e.printStackTrace();
							}
						}
						
					}else{
						return;
					}
				}
			});
			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("say"));
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
	}
	
	public static void emit(){
		MySpout.collector.emit(new Values("Hello World!!! : " + count++));
	}
	
	public static void emit(String value){
		MySpout.collector.emit(new Values(value));
	}
	
}
