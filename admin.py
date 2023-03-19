
## Code of  administrative client for Kafka  ##

from confluent_kafka.admin import AdminClient, NewTopic

topic = 'Kafka_ImageClass_Hagar21'
client_id = "admin_hagar100"

conf = {'bootstrap.servers': "34.70.120.136:9094,35.202.98.23:9094,34.133.105.230:9094",
        'client.id': client_id}

ac = AdminClient(conf)
res = ac.create_topics([NewTopic(topic, num_partitions=3, replication_factor=2)])
res[topic].result()

print("Creating (Kafka_ImageClass_Hagar21) Topic on Kafka ")