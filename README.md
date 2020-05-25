#  Motivation
-   Design ETL flow by editing configuration to reduce development time
-   Extensible toolset for common ETL operations

# Architecture
![alt architecture](https://s3-ap-northeast-1.amazonaws.com/freedomandy-test/mole_architecture.png)

# Getting Started
1.  Generate fat jar `;project core ;assembly`
2.  Edit your configuration file to set source, sink and related transformations. e.g. [test.conf](https://github.com/freedomandy/mole/blob/master/Core/src/main/resources/test.conf)
3.  Submit the fat jar with below settings
```
--conf spark.executor.extraJavaOptions=-Dconfig.fuction.conf \
--conf 'spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.user.file.write.location.policy.class=alluxio.client.file.policy.MostAvailableFirstPolicy -Dconfig.file={your_config_file}' \
```

