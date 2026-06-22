class VanillaSparkReader(IPlanReader):
    def __init__(self, spark: SparkSession, df: DataFrame):
        self.spark = spark
        self.df = df

    def get_execution_plan(self) -> str:
        # Klasyczny Spark bez Connect zwraca plan przez rozszerzone API mechanizmu SQL
        return self.df._jdf.queryExecution().toString()

    def get_physical_metrics(self) -> Dict[str, Any]:
        # W czystym Sparku mapujemy schemat bez odwołań do Unity Catalog
        metrics = {
            "schema_fields": {f.name: f.dataType.simpleString() for f in self.df.schema},
            "num_files": 0 # W czystym Sparku wyciągamy to z Hadoop FS API, jeśli podano ścieżkę
        }
        return metrics