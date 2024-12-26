import pulsar
from dask import dataframe as dd
import pandas as pd

def process_messages(messages):
    """
    Processes messages using Dask for parallel computation.
    """
    # Create a Pandas DataFrame
    df = pd.DataFrame(messages, columns=["message"])

    # Convert to Dask DataFrame for parallel processing
    dask_df = dd.from_pandas(df, npartitions=2)

    # Add a column for message length
    dask_df = dask_df.assign(message_length=dask_df["message"].map(len, meta=("message", "int")))

    # Compute and return the results
    result = dask_df.compute()
    return result

def run_consumer():
    """
    Consumes messages from Pulsar and processes them in batches using Dask.
    """
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe("chat-topic", subscription_name='chat-subscription')

    print("Chat Application - Consumer")
    print("Listening to messages")

    messages = []
    try:
        while True:
            msg = consumer.receive()
            message_text = msg.data().decode('utf-8')
            print(f"Friend: {message_text}")

            # Collect messages for processing
            messages.append((message_text,))
            consumer.acknowledge(msg)

            # Process messages in batches (e.g., every 5 messages)
            if len(messages) >= 5:
                print("\nProcessing Batch...")
                results = process_messages(messages)
                print(results)
                messages = []  # Clear the batch

    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.close()

if __name__ == "__main__":
    run_consumer()
