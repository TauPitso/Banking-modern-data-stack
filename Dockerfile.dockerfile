FROM debezium/connect:2.2
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.7

