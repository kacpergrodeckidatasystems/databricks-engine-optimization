import logging
from src.auditor.models import ClusterContext
from pyspark.sql import SparkSession

logger = logging.getLogger("APM.Environment")

class EnvironmentProvider:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def determine_cluster_context(self) -> ClusterContext:
        """
        Bada właściwości runtime sesji Spark Connect / Serverless.
        Używa bezpiecznych bloków try-except, zapobiegając AnalysisException.
        """
        context = ClusterContext()
        
        # 1. Sprawdzenie typu klienta (Spark Connect vs Tradycyjny Session)
        is_connect = "pyspark.sql.connect" in str(type(self.spark))
        
        if is_connect:
            # Na Spark Connect (Serverless/Shared) parametry są predefiniowane i zablokowane do odczytu
            context.is_serverless = True
            context.aqe_enabled = True       # Databricks Runtime 13.x+ ma domyślnie włączone AQE
            context.photon_enabled = True    # Serverless domyślnie wymusza silnik Photon
            logger.info("[ENVIRONMENT] Wykryto architekturę Spark Connect (Serverless Safe Mode).")
            return context

        # 2. Fallback dla tradycyjnych klastrów dedykowanych (Single User / Legacy)
        try:
            aqe = self.spark.conf.get("spark.sql.adaptive.enabled", "true")
            context.aqe_enabled = aqe.lower() == "true"
            
            photon = self.spark.conf.get("spark.databricks.photon.enabled", "false")
            context.photon_enabled = photon.lower() == "true"
        except Exception as e:
            logger.warning(f"[ENVIRONMENT] Nie można odczytać konfiguracji sesji, używam domyślnych: {str(e)}")
            
        return context