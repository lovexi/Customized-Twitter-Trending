package com.twittertrending;
/*************************************************************************
 * @Yangyang Liu                                                         *
 * 3 Feb 2016                                                            *
 * This is formatted outcomes for trending.                              *
 *************************************************************************/

import storm.trident.operation.builtin.Debug;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class DisplayTrending extends BaseFilter{

  @Override
  public void cleanup(){
  }

  @Override
  public void prepare(Map conf, TridentOperationContext context){
  }

  @Override
  public boolean isKeep(TridentTuple tuple){
    System.out.println("#" + tuple.get(0) + "     Numbers:"  + tuple.get(1));
    return true;
  }

}
