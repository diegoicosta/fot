# From-Offset-Tool (FOT)

POC that demonstrates how to set a specific offset number to a topic consumer and then copy the events from that point to another topic

The idea behind is, suppose you have an app that is consuming a certain topic but then you stop this consumer for some reason. 
This application has an application-id and each partition ended up with a specific offset number (that's the essence of how Kafka consumer works, right?)

What this tools does is, it identifies those offsets number, create a consumer  and start to consume the topic from that point in time copying the data to a different topic. Cool, don't you think (I think it is) ?

In the `src/main/resources/application.properties` there are the definitions of the source (app application-id, topic name) and target (the new application-id, client-id and the topic name to where the data will be copied)
