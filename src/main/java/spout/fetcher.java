package spout;



import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;
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

public class fetcher extends BaseRichSpout {
  SpoutOutputCollector _collector;
 


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() 
  {
     
	  try
      {
		  Mongo mc = new Mongo("localhost",27017);
	      DB db = mc.getDB("trending");
	      DBCollection coll = db.getCollection("tweets");
	      DBCursor cur = coll.find();
	      DBObject obj = cur.next();
	        
          while(cur.hasNext())
	      {
              obj = cur.next();
              System.out.println(obj);
              _collector.emit(new Values(obj));
          }
          
      }
	  catch(Exception e)
	  {
		  
		  
	  }
      
      
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) 
  {
    declarer.declare(new Fields("fetch"));
  }

}
