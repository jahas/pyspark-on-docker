import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)


schema = StructType([
    StructField("id", IntegerType()),
    StructField("Name", StringType()),
    StructField("NIP", StringType()),
    StructField("Address", StringType()),
    StructField("P24Id", IntegerType()),
    StructField("HotresId", IntegerType()),
    StructField("Verified", StringType())
])

dfPartners = sqlContext.read.csv("data/Partners.csv", sep="\t", header=True, schema=schema)
dfPartners.registerTempTable("dfPartners")
# dfPartners.show()

dfTransactions = sqlContext.read.csv("data/Transactions.csv", sep="\t", header=True)
dfTransactions.registerTempTable("dfTransactions")
# dfTransactions.show()

dfNew = sqlContext.sql("""
select dfp.*, (
    select count(*) 
    from dfTransactions dft 
    where dft.hotresid = dfp.HotresId
    ) transactions
from dfPartners dfp
""")
dfNew.show()
