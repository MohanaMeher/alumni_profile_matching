from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("sample.json")

def _eparams(rec):
    degrees, schools, fields_of_study = [], [], []
    for row in rec:
        if row['degree_name'] and row['school'] and row['field_of_study']:
            degrees.append(row['degree_name'])
            schools.append(row['school'])
            fields_of_study.append(row['field_of_study'])
    degrees = [x.strip() for x in degrees]
    schools = [x.strip() for x in schools]
    fields_of_study = [x.strip() for x in fields_of_study]
    numDegrees = len(degrees)
    if numDegrees > 0:
        recent_degree = degrees[0]
        recent_field_of_study = fields_of_study[0]
        recent_school = schools[0]
        return (str(numDegrees), recent_degree, recent_field_of_study, recent_school, "|".join(degrees[1:]), "|".join(fields_of_study[1:]),  "|".join(schools[1:]))
    else:
        return (str(0), None, None, None, None, None, None)

udf_edu = udf(lambda x : _eparams(list(x)), ArrayType(StringType()))
               
df = df.withColumn("languages", concat_ws(" ",col("languages")))\
    .withColumn("edu_params", udf_edu(col("education")))\
    .select('public_identifier', 'full_name', 'headline', 'summary', 
            'country', 'country_full_name', 'city', 'state', 
            'languages', col('edu_params')[0].alias('numDegrees'),
              col('edu_params')[1].alias('recent_degree'),
              col('edu_params')[2].alias('recent_field_of_study'),
              col('edu_params')[3].alias('recent_school'),
              col('edu_params')[4].alias('previous_degrees'),
              col('edu_params')[5].alias('previous_fields_of_study'),
              col('edu_params')[6].alias('previous_schools')
    )


df.show(3, truncate=False)