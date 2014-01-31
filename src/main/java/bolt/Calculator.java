package bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.DBAddress;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

public class Calculator extends BaseRichBolt 
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
              Mongo mc = new Mongo("localhost",27017);
              DB db = mc.getDB("trending");
              DBCollection coll = db.getCollection("result");
              Object jsonObj = tuple.getValue(0); 
              Object o = com.mongodb.util.JSON.parse(jsonObj.toString());
              DBObject dbObj = (DBObject) o;
    		  coll.insert(dbObj);
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
