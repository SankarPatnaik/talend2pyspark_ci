# Talend2PySpark

Convert Talend jobs (JAR) into PySpark code.

## Features
- Extract Talend JAR metadata
- Map tMap → PySpark `withColumn`
- Support tJoin, tAggregateRow

## Usage
```bash
python -m talend2pyspark.cli --jar myjob.jar --output ./pyspark_code
```
