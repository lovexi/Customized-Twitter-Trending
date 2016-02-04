# Customized-Twitter-Trending

## Project Instruction

This is a project based on Apache Storm platform. It performs trending functions for twitter data.

## Configurations

You need to set up some parameters for this applications (All in constants.java file):
* ACCESSTOKENSECRET, ACCESSTOKEN, CONSUMERSECRET and CONSUMERKEY. You need to get those accesses from twitter developer webiste. Build your own applicaiton and get all required tokens.
* Keywords. These keywords are for filtering among all topics in twitter.

The running command line is listed below:
```
mvn compile exec:java -Dstorm.topology=com.twittertrending.TwitterTrendingTopology
```
