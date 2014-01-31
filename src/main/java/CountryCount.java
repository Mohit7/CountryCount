

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import bolt.Extractor;
import bolt.Calculator;
import spout.fetcher;


public class CountryCount {

  public static void main(String[] args) throws Exception 
  {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("fetch", new fetcher(), 10);
    builder.setBolt("extract",  new Extractor(), 5).shuffleGrouping("fetch");
    builder.setBolt("calc",  new Calculator(), 5).shuffleGrouping("extract");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) 
    {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
