import pulsar

def run_producer():
    client = pulsar.Client('pulsar://localhost:6650')
    producer = client.create_producer(
        'persistent://public/default/chat-topic'  # Use the full topic name
    )

    print("Chat Application - Producer")
    print("Type messages to send (type 'exit' to quit):")

    while True:
        message = input("You: ")
        if message.lower() == 'exit':
            break
        producer.send(message.encode('utf-8'))
        print("Message Sent")

    client.close()

if __name__ == "__main__":
    run_producer()
