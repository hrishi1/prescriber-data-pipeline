import logging
import logging.config
#import pandas

#Load the logging config file and create a customer logger for current module
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__) #__name__ gives us the name of current module (validations)


def get_curr_date(spark):
    try:
        df = spark.sql("""SELECT current_date;""")
        logger.info("Validating the spark object by printing current date: "+str(df.collect()))

    except NameError as exp:
        logger.error("NameError in method get_curr_date(): "+str(exp), exc_info=True)
        raise #Sends the error back to parent class

    #Default catch block for all exceptions other than the ones handled above
    except Exception as exp:
        logger.error("Error in method get_curr_date(): "+str(exp), exc_info=True)
        raise #Sends the error back to parent class

    #Executed when there are no exceptions in the code
    else:
        logger.info("Spark object is validated.")

def df_count(df,dfName):
    try:
        logger.info(f"df_count() started for data frame {dfName}...")
        df_count=df.count()
        logger.info(f"The data frame count is {df_count}")

    except Exception as exp:
        logger.error("Error in df_count(): "+ str(exp))
        raise

    else:
        logger.info(f"Dataframe validation by count is completed.")

def df_top10_rec(df,dfName):
    try:
        logger.info(f"df_top10_rec() started for data frame {dfName}... ")
        logger.info("Top 10 records are: ")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t'+df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in df_top10_rec(): "+str(exp))
        raise
    else:
        logger.info("Data frame validation by top 10 records completed.")

def df_print_schema(df,dfName):
    try:
        logger.info(f"The Dataframe Schema validation for {dfName}...")
        sch = df.schema.fields
        logger.info(f"The dataframe schema for {dfName} is: ")
        for i in sch:
            logger.info(f"\t{i}")

    except Exception as exp:
        logger.error("Error in method df_print_schema(): "+str(exp))
        raise
    else:
        logger.info("Dataframe schema validation completed.")
