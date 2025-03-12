from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col
import re

# Initialize Spark Session
spark = SparkSession.builder.appName("ExtractParagraphs").getOrCreate()

def extract_specific_paragraphs(filepath):
    """
    Extracts specific paragraphs and details from a structured text document using Spark.
    
    Args:
        filepath (str): The path to the CSV file.
    
    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with selected paragraph data.
    """
    try:
        df = spark.read.option("header", "true").csv(filepath)
    except Exception as e:
        print(f"Error: {e}")
        return None

    if len(df.columns) != 1:
        print("Error: CSV file must have a single column.")
        return None

    original_column_name = df.columns[0]
    
    # Define the specific sections to extract
    target_sections = [
        "SITUATION", "MISSION", "EXECUTION", "SUSTAINMENT", "COMMAND AND SIGNAL",
        "POC", "ACKNOWLEDGE", "ANNEXES", "DISTRIBUTION", "CLASSIFICATION"
    ]

    # Extract paragraphs using regex
    df = df.withColumn("Paragraphs", regexp_extract(col(original_column_name), r'(\d+\.\s*.*?)(?=\d+\.|$)', 1))
    
    # Extract paragraph names and filter relevant ones
    df = df.withColumn("ParagraphName", regexp_extract(col("Paragraphs"), r'\*\*([\w\s]+)\*\*', 1))
    df = df.filter(col("ParagraphName").isin(target_sections))
    
    # Clean up extracted content
    df = df.withColumn("Content", regexp_extract(col("Paragraphs"), r'\d+\.\s*(.*)', 1))
    
    return df

# Example usage:
filepath = "Ordered.csv"  # Update this with the actual path
result_df = extract_specific_paragraphs(filepath)

if result_df is not None:
    result_df.write.csv("Ordered_filtered_sections.csv", header=True, mode="overwrite")
    result_df.show(truncate=False)
