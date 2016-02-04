package com.twittertrending;
/*************************************************************************
 * @Yangyang Liu                                                         *
 * 31 Jan 2016                                                           *
 * This is spout for grabing tweets from twitter with specific hashtags. *
 *************************************************************************/
import storm.trident.spout.IBatchSpout;
import storm.trident.operation.TridentCollector;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;
import backtype.storm.Config;

import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.StatusListener;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStreamFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout implements IBatchSpout{
  private final String _accessTokenSecret;
  private final String _accessToken;
  private final String _consumerSecret;
  private final String _consumerKey;
  private final FilterQuery _filterquery;

  private LinkedBlockingQueue<Status> _tweetqueue;
  private TwitterStream _twitterStream;
  private SpoutOutputCollector _collector;

  //Call constructor with default _filterquery for tweets (default is null. will call sample())
  public TwitterSpout(String _accessTokenSecret, String _accessToken,
                      String _consumerSecret, String _consumerKey){
    this(_accessTokenSecret, _accessToken, _consumerSecret, _consumerKey, null);
  }

  //TwitterSpout constructor for token and keys for personal twitter app account
  public TwitterSpout(String _accessTokenSecret, String _accessToken,
                      String _consumerSecret, String _consumerKey, FilterQuery _filterquery){
    if (_accessTokenSecret==null || _accessToken==null || _consumerSecret==null || _consumerKey==null){
      throw new NullPointerException("Twitter Token can not be empty");
    }

    //Setup all attributes
    this._accessTokenSecret = _accessTokenSecret;
    this._accessToken = _accessToken;
    this._consumerKey = _consumerKey;
    this._consumerSecret = _consumerSecret;
    this._filterquery = _filterquery;
  }

  //Declare message fields
  public void declareOutputFields(OutputFieldsDeclarer outputfieldsdeclarer){
    outputfieldsdeclarer.declare(new Fields(Constants.TWEET));
  }

  @Override
  public void open(Map map,TopologyContext topologycontext){
    _tweetqueue = new LinkedBlockingQueue<Status>();

    ConfigurationBuilder _configurationbuilder = new ConfigurationBuilder();
    _configurationbuilder.setOAuthConsumerKey(_consumerKey)
                         .setOAuthConsumerSecret(_consumerSecret)
                         .setOAuthAccessToken(_accessToken)
                         .setOAuthAccessTokenSecret(_accessTokenSecret);
    _twitterStream = new TwitterStreamFactory(_configurationbuilder.build()).getInstance();

    _twitterStream.addListener(new StatusListener() {

      //Add tweets into queue.
      @Override
      public void onStatus(Status status) {
        _tweetqueue.offer(status);
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice sdn) {
      }

      @Override
      public void onTrackLimitationNotice(int i) {
      }

      @Override
      public void onScrubGeo(long l, long l1) {
      }

      @Override
      public void onException(Exception e) {
      }

      @Override
      public void onStallWarning(StallWarning warning) {
      }
    });

    //Add hashtag filter for tweets.
    if (_filterquery==null){
      _twitterStream.sample();
    }
    else{
      _twitterStream.filter(_filterquery);
    }
  }

  //Emit tweet to bolt
  @Override
  public void emitBatch(long batchId, TridentCollector _collector){
    final Status tweet = _tweetqueue.poll();
    if (tweet == null){
      Utils.sleep(100);
    }
    else{
      _collector.emit(new Values(tweet));
    }
  }

  @Override
  public void ack(long batchId) {
  }

  @Override
  public Map getComponentConfiguration() {
    return new Config();
  }

  @Override
  public Fields getOutputFields() {
    return new Fields(Constants.TWEET);
  }

  @Override
  public void close(){
    _twitterStream.shutdown();
  }


}
