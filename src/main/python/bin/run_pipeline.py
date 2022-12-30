import get_var as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec
import sys
import logging
import logging.config
import os
from data_ingest import load_files

#Loading the logging configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
print("Testing git connection")

def main():

    try:
        logging.info("main() has started ...")
        #Create spark object
        spark = get_spark_object(gav.envn, gav.appName)
        #Validate spark object
        get_curr_date(spark)

        #Initiate data_ingest script
        #Load the City file
        for file in os.listdir(gav.staging_dim_city):
            print("File is: "+file)
            file_dir = gav.staging_dim_city + '\\' + file
            print(file_dir)

            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark, file_dir, file_format, header, inferSchema)

        #Validate data_ingest script for city dimension data frame
        df_count(df_city,"df_city")
        df_top10_rec(df_city,"df_city")


        logging.info("run_pipeline.py is Completed")

    except Exception as exp:
        #exc_info generates a stack trace
        logging.error("Error occured in main(): "+str(exp), exc_info=True)
        #Without the below statement exit code is 0 (success) - not a good practise
        sys.exit(1)

if __name__ == "__main__":
    logging.info("run_pipeline.py has Started")
    main()
