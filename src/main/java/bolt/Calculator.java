package bolt;


import java.util.Map;
import java.util.HashMap;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;



public class Calculator extends BaseRichBolt {

	OutputCollector _collector;
    JSONArray dispArray;
    JSONObject temp;
    HashMap<Object, HashMap<Object,Integer>> counter; 
    HashMap<Object, Integer> h;
    
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) 
    {
    		_collector = collector;
    		JSONArray dispArray = new JSONArray();
    		JSONObject temp = new JSONObject();
    		counter = new HashMap<Object,HashMap<Object,Integer>>();   
    }

    @Override
    public void execute(Tuple tuple) 
    {
    	      JSONObject jsonObj = (JSONObject) tuple.getValue(0);
              
              try
              {
            	  h = counter.get(jsonObj.get("topic"));
            	  Integer i = h.get(jsonObj.get("country"));
                  i++;
                  
                  h.put(jsonObj.get("country"),i);
                  counter.put(jsonObj.get("topic"), h);
                  	  
              }
              catch(Exception e)
              {
            	  h = new HashMap<Object,Integer>();
            	  h.put(jsonObj.get("country"),1);
            	  counter.put(jsonObj.get("topic"), h);
              }
                  }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("extract"));
    }


  }