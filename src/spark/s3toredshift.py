# In this script, we read the json file from an S3 location
# Use the flatten() function to get the nested json schema into column names
from pyspark.sql import types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from pyspark.sql.functions import lit
import boto3

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
    
    #for df_col_name in df.columns:
        #df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))
    return df

spark = SparkSession \
    .builder \
    .appName("PySpark") \
    .config("spark.sql.parquet.mergeSchema", "true") \
    .getOrCreate()

input_file_name='input/number.txt'
numeric_col_list=set()
with open(input_file_name) as input_file:
    for event_name in input_file:
        if event_name.endswith('\n'):event_name=event_name[:-1]
        numeric_col_list.add(event_name)

input_file_name='input/event.txt'
event_list=[]
modified_event_map=set()
with open(input_file_name) as input_file:
    for event_name in input_file:
        if event_name.endswith('\n'):event_name=event_name[:-1]
        modified_event_map.add(event_name)

input_file_name='input/columns.txt'
column_list=[]
with open(input_file_name) as input_file:
    for event_name in input_file:
        if event_name.endswith('\n'): event_name = event_name[:-1]
        column_list.append(event_name)
    #print (column_list)

input_file_name='input/boolean_columns.txt'
boolean_list=[]
with open(input_file_name) as input_file:
    for event_name in input_file:
        if event_name.endswith('\n'): event_name = event_name[:-1]
        boolean_list.append(event_name)

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# replace the bucket to the real bucket name
bucket='xxx'
my_bucket = s3.Bucket(bucket)

all_obs=my_bucket.objects.all()

for my_bucket_object in all_obs:
    filename=my_bucket_object.key
    print (filename)
    if filename.endswith('.gz'):
        inputf = 's3a://'+bucket+'/'+filename
        print (inputf)
        segment_logs = spark.read.json(inputf)
        flattened_segment = flatten(segment_logs)
        
        newdf=None
        columns=[]
        omitted=[]
        for col in column_list:
            if has_column(flattened_segment, col):
                columns.append(col)
            else: omitted.append(col)

        newdf=flattened_segment.select(columns)
        for col in omitted:
            if col in numeric_col_list: newdf=newdf.withColumn(col, lit(0))
            else: newdf=newdf.withColumn(col, lit('unknown'))

        for col in boolean_list:
            newdf=newdf.withColumn(col, F.col(col).cast("string"))

        for col in column_list:
            if col in numeric_col_list: newdf=newdf.na.fill({col: 0})
            else: newdf=newdf.na.fill({col: 'unknown'})
        
        newdf.printSchema()
        distinct_column = 'event'
        event_df = newdf.select(distinct_column)
        event_df.groupBy('event').count().show(100,False)
        distinct_columns = event_df.select(distinct_column).distinct().collect()
        distinct_columns = [v[distinct_column] for v in distinct_columns]
        
        print (distinct_columns)
        for event in distinct_columns:
            if event not in modified_event_map: continue
            print (event)
            df=newdf.filter(newdf.event==event)

            #use your own redshift, username and password here
            df.write.format("jdbc") \
                .option("url", "jdbc:redshift://myredshiftcluster:5439/database") \
                .option("user", "user") \
                .option("password", "password") \
                .option("dbtable", event.replace('-','_')) \
                .mode('append').save()
