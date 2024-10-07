import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import month, year

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedshiftSalesProcessor:
    def __init__(self, job_name):
        self.job_name = job_name
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.job_name, getResolvedOptions(sys.argv, ['JOB_NAME']))

    def load_data(self):
        try:
            logger.info("Loading data from Redshift")
            self.product_df = self.glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="product").toDF()
            self.sales_df = self.glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="sales").toDF()
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def process_data(self):
        try:
            logger.info("Processing data")
            joined_df = self.sales_df.join(self.product_df, self.sales_df.product_id == self.product_df.product_id)
            joined_df = joined_df.withColumn("year", year(joined_df.sales_date))
            joined_df = joined_df.withColumn("month", month(joined_df.sales_date))
            self.monthly_sales_df = joined_df.groupBy("year", "month", "product_id").sum("sales_amount")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise

    def save_data(self):
        try:
            logger.info("Saving data back to Redshift")
            monthly_sales_dynamic_frame = DynamicFrame.fromDF(self.monthly_sales_df, self.glueContext, "monthly_sales_dynamic_frame")
            self.glueContext.write_dynamic_frame.from_catalog(frame=monthly_sales_dynamic_frame, database="your_database", table_name="monthly_sales")
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise

    def run(self):
        try:
            self.load_data()
            self.process_data()
            self.save_data()
            self.job.commit()
            logger.info("Job completed successfully")
        except Exception as e:
            logger.error(f"Job failed: {e}")
            self.job.commit()

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    processor = RedshiftSalesProcessor(args['JOB_NAME'])
    processor.run()
