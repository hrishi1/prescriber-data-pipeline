import logging
import logging.config

#Load the logging config file and create a customer logger for current module
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__) #__name__ gives use the name of current module (validations)


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
