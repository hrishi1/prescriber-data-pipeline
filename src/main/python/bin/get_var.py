import os

###Set environment variables

#Indicates whether its a test environment or a production environment
os.environ['envn'] = 'TEST'

#Indicates whether the data to be loaded in spark has headers or not
os.environ['header'] = 'True'

#Indicates whether the schema in data should be inferred or not
os.environ['inferSchema'] = 'True'

###Get environment variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

###Set other variables
appName = 'Prescriber Data Pipeline'
current_path = os.getcwd()

staging_dim_city = current_path + '\..\staging\dimension_city'
staging_fact = current_path + '\..\staging\\fact'