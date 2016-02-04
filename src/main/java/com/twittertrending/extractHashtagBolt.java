package com.twittertrending;
/*************************************************************************
 * @Yangyang Liu                                                         *
 * 31 Jan 2016                                                           *
 * This is bolt for processing tweets with hashtags, then counting       *
 *************************************************************************/
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class extractHashtagBolt extends BaseFunction{
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      Status status = (Status) tuple.get(0);
      for (HashtagEntity hashtag : status.getHashtagEntities()) {
        collector.emit(new Values(hashtag.getText()));
      }
    }

}
