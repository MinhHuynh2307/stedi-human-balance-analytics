import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1678512469002 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1678512469002",
)

# Script generated for node Join
Join_node1678512509169 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=CustomerCuratedZone_node1678512469002,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1678512509169",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1678512509169,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://minh-stedi-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
