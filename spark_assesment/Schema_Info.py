from pyspark.sql.functions import *
from pyspark.sql.types import *

####################################################
# Configure the DB properties here
####################################################
user = "root"
password = "1234"
connection_props = {"host":"localhost","user":user,"password":password,"database":"db1"}

####################################################
# Configure source url here
####################################################
userLink = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users"
messagesLink = "https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages"

####################################################
# DataFrame schema
####################################################
userSchema = StructType([StructField("createdAt", StringType(), True) \
                        ,StructField("updatedAt", StringType(), True) \
                        ,StructField("firstName", StringType(), True) \
                        ,StructField("lastName", StringType(), True) \
                        ,StructField("address", StringType(), True) \
                        ,StructField("city", StringType(), True) \
                        ,StructField("country", StringType(), True) \
                        ,StructField("zipCode", StringType(), True) \
                        ,StructField("email", StringType(), True) \
                        ,StructField("profile", StructType([ \
                                StructField('gender', StringType(), True) \
                                ,StructField('isSmoking', BooleanType(), True) \
                                ,StructField('profession', StringType(), True) \
                                ,StructField('income', StringType(), True) \
                                ])) \
                        ,StructField("subscription", ArrayType(
                             StructType([
                                 StructField("createdAt", StringType(), True)
                                 , StructField('startDate', StringType(), True) \
                                 , StructField('endDate', StringType(), True) \
                                 , StructField('status', StringType(), True) \
                                 , StructField('amount', StringType(), True) \
                                 ])
                         ), True)
                         ])

messageSchema = StructType([StructField("createdAt", StringType(), True) \
                        ,StructField("message", StringType(), True) \
                        ,StructField("receiverId", StringType(), True) \
                        ,StructField("id", StringType(), True) \
                        ,StructField("senderId", StringType(), True)
                         ])

userSchemaDb = StructType([StructField("userId", IntegerType(), True) \
                        ,StructField("Hash_column", StringType(), True) \
                        ,StructField("createdAt", TimestampType(), True) \
                        ,StructField("updatedAt", TimestampType(), True) \
                        ,StructField("city", StringType(), True) \
                        ,StructField("country", StringType(), True) \
                        ,StructField("zipCode", StringType(), True) \
                        ,StructField("email", StringType(), True) \
                        ,StructField('gender', StringType(), True) \
                        ,StructField('isSmoking', StringType(), True) \
                        ,StructField('profession', StringType(), True) \
                        ,StructField('income', StringType(), True) \
                         ])

messageSchemaDb = StructType([StructField("id", IntegerType(), True) \
                        ,StructField("createdAt", TimestampType(), True) \
                        ,StructField("senderId", IntegerType(), True)
                        ,StructField("receiverId", IntegerType(), True) \
                         ])

subscriptionSchemaDb = StructType([StructField("Hash_column", StringType(), True) \
                            ,StructField("createdAt", TimestampType(), True)
                            ,StructField('startDate', TimestampType(), True) \
                            ,StructField('endDate', TimestampType(), True) \
                            ,StructField('status', StringType(), True) \
                            ,StructField('amount', StringType(), True) \
                                 ])