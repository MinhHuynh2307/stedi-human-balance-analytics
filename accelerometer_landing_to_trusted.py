import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1678427886013 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1678427886013",
)

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Privacy Join
PrivacyJoin_node1678428233815 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1678427886013,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="PrivacyJoin_node1678428233815",
)

# Script generated for node Filter By Consent Date
SqlQuery242 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
FilterByConsentDate_node1678678229582 = sparkSqlQuery(
    glueContext,
    query=SqlQuery242,
    mapping={"myDataSource": PrivacyJoin_node1678428233815},
    transformation_ctx="FilterByConsentDate_node1678678229582",
)

# Script generated for node Drop Fields
DropFields_node1678428338824 = DropFields.apply(
    frame=FilterByConsentDate_node1678678229582,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1678428338824",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678503837568 = DynamicFrame.fromDF(
    DropFields_node1678428338824.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678503837568",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678503837568,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://minh-stedi-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedZone_node3",
)

job.commit()
