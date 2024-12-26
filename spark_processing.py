from dask import dataframe as dd
import pandas as pd

def process_messages_dask():
    """
    Demonstrates message processing using Dask for parallel computation.
    """
    # Sample data
    data = [("Hello there!",), ("How are you?",), ("Pulsar with Dask!",)]
    columns = ["message"]

    # Create a Pandas DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Convert to Dask DataFrame for parallel processing
    dask_df = dd.from_pandas(df, npartitions=2)

    # Add a column for message length
    dask_df = dask_df.assign(message_length=dask_df["message"].map(len, meta=("message", "int")))

    # Compute and display results
    result = dask_df.compute()
    print("Processed Results:")
    print(result)

if __name__ == "__main__":
    process_messages_dask()
