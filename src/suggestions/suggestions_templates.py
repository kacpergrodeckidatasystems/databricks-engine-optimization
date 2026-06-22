# No imports here is desired - this is a pure data module (Data Module).

TEMPLATES = {
    "PERF-001": {
        "text": "Run data compaction operation (OPTIMIZE) on Delta Lake table, or apply write thread balancing instruction.",
        "code": """spark.sql("OPTIMIZE {table_name}")
# Or in PySpark DataFrame before write:
df.coalesce(1)"""
    },
    "PERF-002": {
        "text": "Force broadcast strategy for smaller dimension table to eliminate Shuffle phase.",
        "code": """from pyspark.sql.functions import broadcast
df_final = df_heavy.join(broadcast(df_light), "key")"""
    },
    "PERF-003": {
        "text": "Remove explicit type casting in filter conditions to enable Data Skipping indexes.",
        "code": """# Instead of:
df.filter(F.col("temperature").cast("string") == "25")
# Use correct data type:
df.filter(F.col("temperature") == 25)"""
    },
    "PERF-004": {
        "text": "Remove physical partitioning by high-cardinality columns. Apply Liquid Clustering.",
        "code": """# Instead of partitionBy:
# df.write.partitionBy("timestamp").format("delta").save(table_path)

# Use CLUSTER BY in Delta Lake:
spark.sql('''
    CREATE TABLE {table_name}
    CLUSTER BY (date_column, category)
    AS SELECT * FROM source_table
''')"""
    }
}
