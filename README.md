# yet-another-twitter-kafka-example
This is me downloading tweets in real time with the Streaming API, which I access through the Java library Hosebird Client, and saving them in a MongoDB, all mediated through Apache Kafka brokers that make things great.

### To-Do List
* MongoDB \_id should be the tweet id.
* Robust recovery: if consumer crashes, it should go back to the last streamed tweet that has been successfully stored in the db. To do so, it can look at the last ID inserted in MongoDB, at the last offset committed by Kafka and at the current offset.
* Deploy to Amazon EC2 machines.
