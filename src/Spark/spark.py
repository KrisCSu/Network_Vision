from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
import os


db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')

region = 'us-east-2'
bucket = 'kris-insight'
key = 'friday.csv'



def sessionizer(df):

	last_time = df.withColumn("last_time", lag('time').over(Window.partitionBy('source').orderBy('time')))

	lag_in_sec = last_time.withColumn('lag_in_sec', (last_time['time']-last_time['last_time']))

	new_session = lag_in_sec.withColumn('is_new_session', when(col('lag_in_sec')>300, 1).otherwise(0))

	new_df = new_session.withColumn('session_id', sum('is_new_session').over(Window.partitionBy('Source').orderBy('Seq')))

	df = new_df.select('seq', 'time', 'source', 'destination', 'protocol', 'length', 'info', 'session_id').orderBy('Seq')

	return df


def write_db(df)
	df.write \
	        .format("jdbc") \
	        .option("url" , "jdbc:postgresql://10.0.0.13:5431/network_db") \
	        .option("dbtable", "network") \
	        .option("user", db_user) \
	        .option("password", db_password) \
	        .option("driver","org.postgresql.Driver") \
	        .mode('overwrite') \
	        .save()

if __name__ == '__main__':
	sc = SparkContext()
	sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')

	spark = SparkSession(sc)
	s3file = f's3a://{bucket}/{key}'
	df = spark.read.csv(s3file, header='True')

	# "Seq","Time","Source","Destination","Protocol","Length","Info"
	df = df \
	        .withColumn('seq', df['Seq'].cast('int')) \
	        .withColumn('time', df['Time'].cast(FloatType())) \
	        .withColumn('length', df['Length'].cast('int'))

	sessionizer(df)
	write_db(df)

	spark.stop()
