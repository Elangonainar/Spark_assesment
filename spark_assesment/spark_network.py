from pyspark.sql import SparkSession
import urllib.request
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import mysql.connector
from Schema_Info import *
from functools import partial

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

sc = spark.sparkContext
brConnect = sc.broadcast(connection_props)

####################################################
# This method is to Extract data from HTTPS URL
####################################################
def getDataFromLink(url):
    f = urllib.request.urlopen(url)
    extractedData = f.read().decode("utf-8")
    extractedData = json.loads(extractedData)
    return extractedData

####################################################
# This method is to Get Connection from Database
####################################################
def get_db_connection():
    database = connection_props.get("database")
    user = connection_props.get("user")
    pwd = connection_props.get("password")
    host = connection_props.get("host")

    db_connection = mysql.connector.connect(
            host = host,
            user = user,
            password = pwd,
            database = database
        )
    return db_connection

################################################################
# This method is to Persist dataframe into Data base row by row
################################################################
def persist_row(table, tran_type, row, cur_merge):
    if (table == 'users'):
        Hash_column = row.__getitem__("Hash_column")
        createdAt = row.__getitem__("createdAt")
        updatedAt = row.__getitem__("updatedAt")
        city = row.__getitem__("city")
        country = row.__getitem__("country")
        zipcode = row.__getitem__("zipcode")
        email_domain = row.__getitem__("email_domain")
        gender = row.__getitem__("gender")
        issmoking = row.__getitem__("issmoking")
        profession = row.__getitem__("profession")
        income = row.__getitem__("income")

        if (tran_type == 'Insert'):
            sql_string = f"""INSERT INTO DB1.users (Hash_column, createdAt, updatedAt, city, country, zipcode, email_domain, gender, issmoking, profession, income)
            VALUES ('{Hash_column}','{createdAt}','{updatedAt}','{city}','{country}','{zipcode}','{email_domain}','{gender}','{issmoking}','{profession}','{income}')"""
        else:
            sql_string = f"""UPDATE DB1.users SET createdAt = '{createdAt}',updatedAt = '{updatedAt}', city = '{city}', country = '{country}', zipcode = '{zipcode}',
            email_domain = '{email_domain}', gender = '{gender}', issmoking = '{issmoking}',profession = '{profession}', income = '{income}'
            where Hash_column = '{Hash_column}'"""

    elif (table == 'subscription'):
        Hash_column = row.__getitem__("Hash_column")
        createdAt = row.__getitem__("createdAt")
        startDate = row.__getitem__("startDate")
        endDate = row.__getitem__("endDate")
        status = row.__getitem__("status")
        amount = row.__getitem__("amount")

        if (tran_type == 'Insert'):
            sql_string = f"""INSERT INTO DB1.subscription (Hash_column, createdAt, startDate, endDate, status, amount)
                VALUES ('{Hash_column}','{createdAt}','{startDate}','{endDate}','{status}','{amount}')"""
        else:
            sql_string = f"""UPDATE DB1.subscription SET createdAt = '{createdAt}',startDate = '{startDate}', endDate = '{endDate}', status = '{status}', amount = '{amount}'
            where Hash_column = '{Hash_column}'"""

    else:
        id = row.__getitem__("id")
        createdAt = row.__getitem__("createdAt")
        senderId = row.__getitem__("senderId")
        receiverId = row.__getitem__("receiverId")

        if (tran_type == 'Insert'):
            sql_string = f"""INSERT INTO DB1.message (id, createdAt, senderId, receiverId)
                VALUES ('{id}','{createdAt}','{senderId}','{receiverId}')"""
        else:
            sql_string = f"""UPDATE DB1.message SET createdAt = '{createdAt}',senderId = '{senderId}', receiverId = '{receiverId}'
            where id = '{id}'"""

    cur_merge.execute(sql_string)


########################################################################
# This method opens database connection and executes persist_row method
########################################################################
def process_partition(table, tran_type, partition):
    db_conn = get_db_connection()
    dbc_merge = db_conn.cursor()

    # for row in partition:
    persist_row(table, tran_type, partition, dbc_merge)

    db_conn.commit()
    dbc_merge.close()
    db_conn.close()

############################################################
# Exract data from source URL and Dataframe has been created
############################################################
userData = getDataFromLink(userLink)
messagesData = getDataFromLink(messagesLink)

userRawDf = spark.createDataFrame(userData, schema = userSchema)
userDf = userRawDf \
    .withColumn("Hash_column",md5(concat_ws(",",col("firstName"),col("lastName"),col("email")))) \
    .withColumn("issmoking", when(userRawDf.profile["issmoking"] == True, 1).otherwise(0)) \
    .withColumn("email_domain", regexp_extract(col("email"), "(?<=@)[^.]+(?=\\.)", 0)) \
    .select("Hash_column",to_timestamp(col("createdAt")).alias("createdAt"),to_timestamp(col("updatedAt")).alias("updatedAt"),"city","country","zipcode","email_domain", \
            userRawDf.profile["gender"].alias("gender"), userRawDf.profile["issmoking"].alias("issmoking"), userRawDf.profile["profession"].alias("profession") \
            ,userRawDf.profile["income"].alias("income"),"subscription")
subscriptionRawDf = userDf.select("Hash_column",explode(userRawDf.subscription).alias("subscription"))
subscriptionDf = subscriptionRawDf.select("Hash_column", to_timestamp(subscriptionRawDf.subscription["createdAt"]).alias("createdAt") \
                                          ,to_timestamp(subscriptionRawDf.subscription["startDate"]).alias("startDate") \
                                          ,to_timestamp(subscriptionRawDf.subscription["endDate"]).alias("endDate") \
                                          ,subscriptionRawDf.subscription["status"].alias("status"), subscriptionRawDf.subscription["amount"].alias("amount"))
messageDf = spark.createDataFrame(messagesData, schema = messageSchema)

############################################################
# Exract data Database and Dataframe has been created
############################################################
db_conn = get_db_connection()
dbc_merge = db_conn.cursor()

usersQuery = "SELECT * FROM users"
messageQuery = "SELECT * FROM message"
subscriptionQuery = "SELECT * FROM subscription"

usersPdf = pd.read_sql(usersQuery, con=db_conn)
messagePdf = pd.read_sql(messageQuery, con=db_conn)
subscriptionPdf = pd.read_sql(subscriptionQuery, con=db_conn)
db_conn.close()

originalUsersDf = spark.createDataFrame(usersPdf, schema = userSchemaDb)
originalmessageDf = spark.createDataFrame(messagePdf, schema = messageSchemaDb)
originalsubscriptionDf = spark.createDataFrame(subscriptionPdf, schema = subscriptionSchemaDb)

##################################################################
# Categorizing the new arrival data and updated data as Dataframes
##################################################################
usersNewRecordDf = userDf.join(originalUsersDf, userDf.Hash_column == originalUsersDf.Hash_column, "left") \
                    .where(originalUsersDf.Hash_column.isNull())
subscriptionNewRecordDf = subscriptionDf.join(originalsubscriptionDf, (subscriptionDf.Hash_column == originalsubscriptionDf.Hash_column) & \
                                              (date_trunc("minute", originalsubscriptionDf.createdAt) == date_trunc("minute", subscriptionDf.createdAt)), "left") \
                    .where(originalsubscriptionDf.Hash_column.isNull())
messageNewRecordDf = messageDf.join(originalmessageDf, messageDf.id == originalmessageDf.id, "left") \
                    .where(originalmessageDf.id.isNull()) \
                    .select(to_timestamp(messageDf.createdAt).alias("createdAt"),messageDf.receiverId.cast("int").alias("receiverId") \
                            ,messageDf.senderId.cast("int").alias("senderId"), messageDf.id.cast("int").alias("id"))

usersUpdateRecordDf = userDf.join(originalUsersDf, userDf.Hash_column == originalUsersDf.Hash_column, "left") \
                    .where((originalUsersDf.Hash_column.isNotNull()) & (date_trunc("minute", originalUsersDf.updatedAt) != date_trunc("minute", userDf.updatedAt))) \
                    .select(userDf.Hash_column,to_timestamp(userDf.createdAt).alias("createdAt"),to_timestamp(userDf.updatedAt).alias("updatedAt"),userDf.city, \
                            userDf.country, userDf.zipcode, userDf.email_domain, userDf.gender, userDf.issmoking, userDf.profession, \
                            userDf.income , userDf.subscription)

subscriptionUpdateRawDf  = usersUpdateRecordDf.join(subscriptionDf, usersUpdateRecordDf.Hash_column == subscriptionDf.Hash_column).select(subscriptionDf["*"])
subscriptionUpdateRecordDf = subscriptionUpdateRawDf \
                    .join(originalsubscriptionDf, (subscriptionUpdateRawDf.Hash_column == originalsubscriptionDf.Hash_column) & \
                                              (date_trunc("minute", originalsubscriptionDf.createdAt) == date_trunc("minute", subscriptionUpdateRawDf.createdAt)), "left") \
                    .where((originalsubscriptionDf.Hash_column.isNotNull()))


##################################################################
# Persisting the final DataFrame into DB
##################################################################
usersNewRecordDf.rdd.coalesce(5).foreach(partial(process_partition,'users','Insert'))
subscriptionNewRecordDf.rdd.coalesce(5).foreach(partial(process_partition,'subscription','Insert'))
messageNewRecordDf.rdd.coalesce(5).foreach(partial(process_partition,'message','Insert'))

usersUpdateRecordDf.rdd.coalesce(5).foreach(partial(process_partition,'users','update'))
subscriptionUpdateRecordDf.rdd.coalesce(5).foreach(partial(process_partition,'subscription','update'))


