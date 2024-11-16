from kafka import KafkaConsumer

# Configuration
KAFKA_HOST = "localhost"
KAFKA_PORT = "9092"

KAFKA_TOPIC = "order-events"
KAFKA_BOOTSTRAP_SERVERS_CONS = KAFKA_HOST + ':' + KAFKA_PORT


if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    
    # Connect to kafka using bootstrap server
    try:
        consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8'))

        for message in consumer:
            #print(dir(message))
            message = message.value
            print("Message received: ", message)
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)