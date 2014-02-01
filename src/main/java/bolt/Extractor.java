package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.DBObject;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Extractor extends BaseRichBolt 
{
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) 
    {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) 
    {
    	
    	try
        {
         	  JSONParser parser=new JSONParser();
              DBObject obj = (DBObject) tuple.getValue(0);
              
              String topic = new String();
              topic = obj.get("topic").toString();
              
              
              obj = (DBObject)obj.get("raw"); 
              obj = (DBObject)obj.get("statuses");
              
              JSONArray array = null;
			  array = (JSONArray) parser.parse(obj.toString());
			  JSONObject emitobj = new JSONObject();
              emitobj.put("topic", topic);
              
              
              
              String tz = new String();
              try
              {
              
	              for(int i=0 ; i < array.size();i++)
	              {
	              	JSONObject jobj =(JSONObject) array.get(i);
	              	emitobj.put("tweet",jobj.get("text"));
	              	
	              	//timezone logic
	              	tz = ((JSONObject)jobj.get("user")).get("time_zone").toString();
	              	if(tz == "None")
	              		tz = ((JSONObject)jobj.get("user")).get("location").toString();
	              	if(tz == "None")
	              		tz = jobj.get("place").toString();
	              	if(tz == "None")
	              		tz = "unknown";
	              		
	              }
	          }
              catch(Exception e)
              {
            	  tz = "unknown";
              }
              
              
              
             
                  	
              	
              emitobj.put("country",tz);
              	
              _collector.emit(new Values(emitobj));
              
              
        }
    	catch(Exception e)
    	{
    		System.out.println("UnknownHostException: "+e);
      		
    	}

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
      declarer.declare(new Fields("extract"));
    }


 }