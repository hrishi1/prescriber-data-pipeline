import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, when, isnan, avg, round, coalesce
from pyspark.sql.window import Window

#Load logging configuration file
logging.config.fileConfig(fname = '../util/logging_to_file.conf')
logger = logging.getLogger(__name__)

#clean df_city dataframe
def perform_data_clean(df1, df2):


    #Clean df_city
    try:
        logger.info(f"perform_data_clean() started ...")
        logger.info(f"pre-processing for df_city started ...")

        # 1. Select only required column
        df_city_sel = df1.select(df1.city,
                                 df1.state_id,
                                 df1.state_name,
                                 df1.county_name,
                                 df1.population,
                                 df1.zips)

        # 2. Convert city, state and county fields to upper case
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips
                                 )

        #Clean df_fact
        logger.info(f"pre-processing for df_fact started ...")

        # 1. Select only required columns
        # 2. Rename the columns
        df_fact_sel = df2.select(df2.npi.alias("presc_id"), df2.nppes_provider_last_org_name.alias("presc_lname"), \
                                 df2.nppes_provider_first_name.alias("presc_fname"),
                                 df2.nppes_provider_city.alias("presc_city"), \
                                 df2.nppes_provider_state.alias("presc_state"),
                                 df2.specialty_description.alias("presc_spclt"), df2.years_of_exp, \
                                 df2.drug_name, df2.total_claim_count.alias("trx_cnt"), df2.total_day_supply, \
                                 df2.total_drug_cost)

        # 3. Add a country field 'USA'
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))

        # 4. Clean years_of_exp field using regex
        pattern = '\d+' #This selects one or more digits from the string
        idx = 0 #Start checking from index 0

        df_fact_sel = df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"), pattern, idx))

        # 5. Convert datatype from String to Number for years_of_exp
        df_fact_sel = df_fact_sel.withColumn("years_of_exp",col("years_of_exp").cast("int"))

        # 6. Combine first_name and last_name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

        # 7. Check and clean all the Null/Nan values
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()

        # 8. Delete records where PRESC_ID is NULL
        df_fact_sel = df_fact_sel.dropna(subset = 'presc_id')

        # 9. Delete records where DRUG_NAME is NULL
        df_fact_sel = df_fact_sel.dropna(subset='drug_name')

        # 10. Impute TRX_CNT where it is null as average of TRX_CNT
        spec = Window.partitionBy('presc_id')
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce('trx_cnt', round(avg('trx_cnt').over(spec))))
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', col('trx_cnt').cast('integer'))

        # Checking for NULLs again
        df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()


    except Exception as exp:
        logger.error("Error in the method - data_preprocessing(): "+str(exp), exc_info=True) #True to get the entire stack trace
        raise
    else:
        logger.info("perform_data_clean() completed.")
    return df_city_sel, df_fact_sel

