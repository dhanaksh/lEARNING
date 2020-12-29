from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('union').master("local[1]").getOrCreate()
col=('name','age')
da=[('chirag',23),
    ('daksh',23),
    ('vivek',23)
    ]
df=spark.createDataFrame(data=da,schema=col)
df.show()