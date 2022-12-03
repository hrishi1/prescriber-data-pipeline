from pyspark.sql import SparkSession
import logging
import logging.config

#Load the logging config file and create a customer logger for current module
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__) #__name__ gives use the name of current module (create_objects)

def get_spark_object(envn, appName):
    try:
        logger.info(f"get_spark_object() has started. Using {envn} envn.")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession \
            .builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()

    except NameError as exp:
        logging.error("NameError in get_spark_object(): "+str(exp), exc_info=True)
        raise

    except Exception as exp:
        logging.error("Error in get_spark_object(): "+str(exp), exc_info=True)

    else:
        logging.info("Spark object is created...")

    return spark




