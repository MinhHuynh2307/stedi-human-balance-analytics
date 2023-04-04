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

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1678513167583 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1678513167583",
    )
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrustedZone_node1",
)

# Script generated for node Join
Join_node1678513555754 = Join.apply(
    frame1=StepTrainerTrustedZone_node1,
    frame2=AccelerometerTrustedZone_node1678513167583,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1678513555754",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1678513555754,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://minh-stedi-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
