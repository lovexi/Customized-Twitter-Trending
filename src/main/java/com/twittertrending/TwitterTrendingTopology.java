package com.twittertrending;
/*************************************************************************
 * @Yangyang Liu                                                         *
 * 3 Feb 2016                                                            *
 * This is basic topology setups for storm trending                      *
 *************************************************************************/
import storm.trident.TridentTopology;
import storm.trident.spout.IBatchSpout;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FirstN;
import backtype.storm.topology.base.BaseRichSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;

import twitter4j.FilterQuery;

import java.io.IOException;

public class TwitterTrendingTopology{

  public static StormTopology buildTopology(IBatchSpout spout) throws IOException {
    final TridentTopology topology = new TridentTopology();
    topology.newStream(Constants.SPOUT, spout)
    .each(new Fields(Constants.TWEET), new extractHashtagBolt(), new Fields(Constants.HASHTAG))
    .groupBy(new Fields(Constants.HASHTAG))
    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(Constants.COUNT))
    .newValuesStream()
    .applyAssembly(new FirstN(Constants.FIRSTN, Constants.COUNT))
    .each(new Fields(Constants.HASHTAG, Constants.COUNT), new DisplayTrending());
    //Build and return the topology
    return topology.build();
  }

  public static void main(String[] args) throws Exception{
    Config conf = new Config();
    FilterQuery filterquery = new FilterQuery();
    filterquery.track(new String[]{Constants.KEYWORDS[0], Constants.KEYWORDS[1], Constants.KEYWORDS[2]});

    /*Initialize sequence should be
     *TwitterSpout(accessTokenSecret, accessToken, consumerSecret, consumerKey)
     */
    IBatchSpout spout = new TwitterSpout(Constants.ACCESSTOKENSECRET, Constants.ACCESSTOKEN
    , Constants.CONSUMERSECRET, Constants.CONSUMERKEY, filterquery);

    if(args.length==0) {
      //create LocalCluster and submit
      final LocalCluster local = new LocalCluster();
      try {
        local.submitTopology("hashtag-count-topology", conf, buildTopology(spout));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      //If on a real cluster, set the workers and submit
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(spout));
    }
  }
}
