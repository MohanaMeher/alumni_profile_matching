import sys
from mongodb import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from user_definition import *
from datetime import date


def _eparams(rec):
    if rec is None:
        return (None, None, None, None, None, None, None)
    rec = list(rec)
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
        return (str(numDegrees), recent_degree, recent_field_of_study, recent_school, ",".join(degrees[1:]), ",".join(fields_of_study[1:]),  ",".join(schools[1:]))
    return (str(0), None, None, None, None, None, None)


def _expparams(rec):
    if rec is None:
        return (None, None, None, None)
    rec = list(rec)
    titles, companies, descriptions = [], [], []
    work_exp = 0
    for row in rec:
        if row['title']:
            titles.append(row['title'])
        if row['description']:
            descriptions.append(row['description'])
        if row['company']:
            companies.append(row['company'])
            s = row['starts_at']
            e = row['ends_at']
            if s['year'] and s['month'] and e['month'] and e['year']:
                d1 = date(s['year'], s['month'], 1)
                d2 = date(e['year'], e['month'], 1)
                delta = d2 - d1
                work_exp += (delta.days)
    return (','.join(titles), ','.join(companies), str(work_exp // 360), "\n".join(descriptions))


def _params(rec, field):
    if rec is None:
        return ''
    return ",".join([p[field] for p in list(rec)])


### UDF Functions ###
udf_volunteer_titles = udf(lambda x: _params(x, 'title'), ArrayType(StringType()))
udf_edu = udf(lambda x : _eparams(x), ArrayType(StringType()))
udf_exp = udf(lambda x : _expparams(x), ArrayType(StringType())) 
udf_certification = udf(lambda x: _params(x, 'name'), ArrayType(StringType()))


def insert_aggregates_to_mongo(profiles_path_by_cohort):
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    alumni_data = spark.read.format("json")\
        .option("header", False)\
        .load(f"gs://{bucket_name}/{profiles_path_by_cohort}")
    df = alumni_data.withColumn("languages", concat_ws(" ",col("languages")))\
        .withColumn("edu_params", udf_edu(col("education")))\
        .withColumn("volunteer_work", udf_volunteer_titles(col("volunteer_work")))\
        .withColumn("exp", udf_exp(col("experiences")))\
        .withColumn("certifications", udf_certification(col("certifications")))\
        .select('public_identifier', 'full_name', 'headline', 'summary', 
                'country', 'country_full_name', 'city', 'state', 
                'languages', col('edu_params')[0].alias('numDegrees'),
                col('edu_params')[1].alias('recent_degree'),
                col('edu_params')[2].alias('recent_field_of_study'),
                col('edu_params')[3].alias('recent_school'),
                col('edu_params')[4].alias('previous_degrees'),
                col('edu_params')[5].alias('previous_fields_of_study'),
                col('edu_params')[6].alias('previous_schools'), 
                "volunteer_work", col('exp')[0].alias('titles'),
                col('exp')[1].alias('companies'), 
                col('exp')[2].alias('total_years_of_experience'), 
                col('exp')[3].alias('exp_descriptions'), "certifications")

    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name,
                                collection_name)
    columns = df.columns
    rdd = df.rdd
    aggregates = rdd.map(lambda x:dict(zip(columns, x)))
    for aggregate in aggregates.collect():
        aggregate['cohort'] = profiles_path_by_cohort.split('/')[-1].split('.')[0]
        mongodb.insert_one(aggregate)

if __name__=="__main__":
    profiles_path_by_cohort = sys.argv[1]
    try:
        insert_aggregates_to_mongo(profiles_path_by_cohort)
    except:
        logging.error(f'Error inserting aggregate record to mongodb')
