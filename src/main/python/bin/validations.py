
def get_curr_date(spark):
    df = spark.sql("""SELECT current_date;""")
    print("Validating the spark object by printing current date: "+str(df.collect()))