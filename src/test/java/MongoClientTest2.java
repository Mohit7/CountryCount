/*
 * Retrieves data from mongo. All the tweets in json format.
 * To be used by the kafka producer and conusmed by a storm-topology.
 */

//package kafka.examples;

import java.net.UnknownHostException;

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

class MongoClientTest2 {
    
    
    public static void main(String[] args) {
        try
          {
                Mongo mc = new Mongo("localhost",27017);
                DB db = mc.getDB("trending");
                DBCollection coll = db.getCollection("tweets");
                DBCursor cur = coll.find();
                DBObject obj = cur.next();
                JSONParser parser=new JSONParser();
                System.out.println("Topic:"+obj.get("topic").toString());
                obj = (DBObject)obj.get("raw");
                System.out.println("Topic:"+obj.toString());
                 
                obj = (DBObject)obj.get("statuses");
                System.out.println("statuses:"+obj.toString());
                
                JSONArray array = (JSONArray) parser.parse(obj.toString());
                JSONObject obj1 = (JSONObject) array.get(0);
                System.out.println("text:"+obj1.get("text"));
                
                for(int i=0 ; i < array.size();i++)
                {
                	JSONObject jobj =(JSONObject) array.get(i);
                	System.out.println("text"+i+jobj.get("text"));
                }
                
          
                
          }

        catch(Exception e)
          {
        		System.out.println("UnknownHostException: "+e);
          }
    }
}