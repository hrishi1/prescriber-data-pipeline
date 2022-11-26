import get_var as gav
from create_objects import get_spark_object
from validations import get_curr_date


def main():

    #Create spark object
    spark = get_spark_object(gav.envn, gav.appName)
    print("Spark object is created...")

    #Validate spark object
    get_curr_date(spark)



if __name__ == "__main__":
    main()
