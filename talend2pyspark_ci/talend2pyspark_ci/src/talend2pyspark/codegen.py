import os

def write_pyspark_code(plan, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, 'job.py')
    with open(out_file, 'w') as f:
        f.write("from pyspark.sql import SparkSession\n")
        f.write("from pyspark.sql.functions import *\n\n")
        f.write("spark = SparkSession.builder.appName('Talend2PySpark').getOrCreate()\n")
        f.write("df = spark.read.csv('input.csv', header=True, inferSchema=True)\n\n")
        for step in plan:
            f.write(f"# {step['type']}\n")
            f.write(step['output'] + "\n\n")
        f.write("spark.stop()\n")
