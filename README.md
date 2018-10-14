#  Motivation
-   Reducing the development time of data synchronize ETL works
-   Data transformation based on Spark computing power
-   To further support CQRS by the flexible ETL works between the query side(web server) and the write side(storage or queue)

# Architecture
![alt architecture](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/mole_architecture.png)

# Getting Started
1.  Edit your configuration file to set source, sink and related transformations
2.  Add the below two configuration settings into your spark submit shell and execute it
```
--conf spark.executor.extraJavaOptions=-Dconfig.fuction.conf \
--conf 'spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.user.file.write.location.policy.class=alluxio.client.file.policy.MostAvailableFirstPolicy -Dconfig.file={your_config_file}' \
```

# Configuration
### Settings
* source
	-   __query__: SQL query, assigning the query string to retrieve data from HIVE
	- __key__: The primary key of the source data set
* sink
    -   __mongoDB__: The url of mongoDB
* transform
	-   __time__: The time format, which used to parsing source value to millisecond timestamp
	-   __flow__: The transform pipeline, which provides different __transform operations__ to custom the transform pipeline

### Transform operations
- Field Mapping <br />
![alt fieldMapping](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/fieldMapping.png)
	-  __Definition__ : Field Mapping provide the flexibility of field name changes and fields projection by using data frame “rename” and “drop” methods
	-  __Input__
		- from: The original field names
		- to: The target field names
- Filling Forward <br />
![alt filling](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/filling.png)
	-  __Definition__ : This transformation provide the ability to fill previous values to the null value fields in a specified column .
	-  __Input__
		-  sessionKey: The filling block key, which used to separate the whole dataset to different blocks, and the filling value operations only affect the null fields in the same block. i.e. the null fields in each block will not be filled values which come from other blocks.
		-  fillFieldIndex: The filling field
		-  sortFieldIndex: The sort field, the transformation will follows this order to fill values
- Group By <br />
![](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/groupby.png)
	-  __Definition__ : This transformation provide the ability to group data by specified key transformation
	-  __Input__
		-  key: The key fields
		-  aggregate: The aggregate operator, which includes ‘sum’, ‘count’, ‘max’, ‘min’ and ‘avg’
		-  aggregateFields: The field which will be aggregated
- Filter <br />
![](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/filtering.png)
	- __Definition__ : The filtering transformation
	- __Input__
		- field: The target field
		- value: The filtering value, which can be assigned a constant value or a specified field
		- operator: The comparing operator which include ‘>’(great) , ‘<’ (less), ‘>=’ (great equal), ‘<=’ (less equal) ,’==’ (equal) and ‘!=’ (not equal)
- Custom <br />
![](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/custom.png)
	- __Definition__ : The customized transformation provide the flexibility to inject the spark user define function(UDF) by giving a Scala function literal.
	- __Parameters__
		- input: The input the UDF function, which allows to assign columns as input values
		- output: The output column name, the output of the function will be assigned to this column
		- outputType: The output column type
		- function: The function literal

#  Example
![alt example](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/example.png)
#  Future Works
-   Expand connectors, such as Kafka, WebHook, CSV and etc.
-   Implement Stream based ETL module
-   Improve the performance of the fill forward value method

