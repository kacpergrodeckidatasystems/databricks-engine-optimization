import logging
from apm_spark_auditor.auditor.models import ClusterContext
from pyspark.sql import SparkSession

logger = logging.getLogger("APM.Environment")


class EnvironmentProvider:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def determine_cluster_context(self) -> ClusterContext:
        """
        Examines runtime properties of Spark Connect / Serverless session.
        Uses safe try-except blocks, preventing AnalysisException.
        """
        context = ClusterContext()

        # 1. Check client type (Spark Connect vs Traditional Session)
        is_connect = "pyspark.sql.connect" in str(type(self.spark))

        if is_connect:
            # On Spark Connect (Serverless/Shared) parameters are predefined and locked for reading
            context.is_serverless = True
            context.aqe_enabled = True  # Databricks Runtime 13.x+ has AQE enabled by default
            context.photon_enabled = True  # Serverless enforces Photon engine by default
            logger.info("[ENVIRONMENT] Detected Spark Connect architecture (Serverless Safe Mode).")
            return context

        # 2. Fallback for traditional dedicated clusters (Single User / Legacy)
        try:
            aqe = self.spark.conf.get("spark.sql.adaptive.enabled", "true")
            context.aqe_enabled = aqe.lower() == "true"

            photon = self.spark.conf.get("spark.databricks.photon.enabled", "false")
            context.photon_enabled = photon.lower() == "true"
        except Exception as e:
            logger.warning(
                f"[ENVIRONMENT] Cannot read session configuration, using defaults: {str(e)}"
            )

        return context
