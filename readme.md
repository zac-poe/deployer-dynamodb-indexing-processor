# Crafter Deployer AWS Processors

## AWS Kinesis

Developed based on the [sample project](https://github.com/aws/aws-sdk-java/tree/master/src/samples/AmazonKinesis)

## Usage

Build de project by running `mvn clean package` and then copy the `target/deployer-aws-processors-{VERSION}.jar` file to `$INSTALL_DIR/bin/crafter-deployer/lib`.

The Kinesis workers and Dynamo Indexing Processor may be used independently. Spring context and Deployer target configuration are only required for the utilized components.  

### Kinesis Workers

The Kinesis worker configuration provides ongoing data indexing for a Kinesis stream. The KinesisWorkerManager comes online and stays online from the time of the context load. This opens active connections for each configured worker using the provided IRecordProcessorFactory.

The KinesisIndexingProcessorFactory will be used as the processorFactory by the worker manager, unless otherwise configured. This factory uses the defined target configuration to create workers which index data records.

### Dynamo Indexing Processor

 The Dynamo Indexing Processor is explicitly invoked through the [Deploy Target](https://docs.craftercms.org/en/3.0/developers/projects/deployer/api/target-management/deploy-target.html) RESTful endpoint.

The `reprocess_all_files` parameter must be `true` for this processor to be triggered.

Optionally, the `dynamo_tables` parameter may be passed. If provided, re-indexing will target only the requested tables, allowing for a targeted subset of re-indexing. This parameter supports either a list of tables, such as `["table1", "table2", "table3"]`, or a single value, like `"table"`.

## Example Configuration

### Target Context

The following beans need to be added to the target context configuration:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
       http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd">

	<!-- kinesis worker management -->
    <bean id="kinesisIndexingProcessorFactory" class="org.craftercms.deployer.aws.kinesis.KinesisIndexingProcessorFactory"/>
    <bean id="kinesisWorkerManager" class="org.craftercms.deployer.aws.kinesis.KinesisWorkerManager"/>

    <!-- dynamo indexing processor -->
    <bean id="dynamoIndexingProcessor" class="org.craftercms.deployer.aws.processor.DynamoIndexingProcessor" parent="deploymentProcessor"/>

</beans>
```

#### KinesisWorkerManager
`processorFactory` - Can be explicitly assigned if an alternative IRecordProcessorFactory implementations is to be used.

Example customization:  
```
    <bean id="myProcessorFactory" class="com.example.MyAlternativeProcessorFactory"/>
    
    <bean id="kinesisWorkerManager" class="org.craftercms.deployer.aws.kinesis.KinesisWorkerManager">
    	<property name="processorFactory" ref="myProcessorFactory"/>
    </bean>
```

### Target Configuration

At least one worker must be defined under `aws.kinesis.workers`. The Kinesis Client Library configuration must follow these restrictions:
- `workerId` must be unique for a given `appName`
- all workers under the same `appName` use the same `stream`
- because `appName` will be used internally as a DynamoDB table it must be unique for the given `region`
- the credentials used must have the required permissions for both Kinesis & DynamoDB (if using Dynamo as the kinesis source)

The `aws.kinesis` section can be configured with the following options:
- `intialPosition` is only needed if the processor should handle all pending records when it starts, the default behaviour is to only receive new ones after it is started. (See https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-additional-considerations.html for additional details.)
- `isDynamo` if set to true, indicates Kinesis workers are connected to and processing DynamoDB streams
- `maxProcessingRetries` indicates maximum number of retries for processing a record set (negative value indicates to retry indefinitely until successful). The default value is 3 retries.
- `maxCheckpointRetries` indicates maximum number of retries for checkpointing (negative value indicates to retry indefinitely until successful). The default value is 10
- `metrics.enabled` if set to true the credentials used need to include write permissions for AWS CloudWatch.
- `metrics.level` is utilized if `metrics.enabled` is true and must be a value from `NONE`, `SUMMARY` or `DETAILED`.

`aws.credentials` & `dynamoIndexingProcessor.credentials` are both optional, if they are not provided the default
credential provider chain will be used. [More info](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)

Both the Kinesis workers and the DynamoDB processor support a boolean configuration `skipFailingRecords` to indicate if they should skip individual records that fail to index instead of retrying the operation. The flag will default to `true` if its not present.

In order to support delete events from DynamoDB the stream must be configured to include the old image of the records.

```yaml
aws:
  credentials:
    accessKey: ...
    secretKey: ...
  region: us-west-1
  kinesis:
    workers:
      - appName: crafter-deployer-table1
        workerId: crafter-deployer-table1-worker-1
        stream: arn:aws:dynamodb:...
      - appName: crafter-deployer-table2
        workerId: crafter-deployer-table2-worker-1
        stream: arn:aws:dynamodb:...
    initialPosition: TRIM_HORIZON
    isDynamo: true
    maxProcessingRetries: -1
    maxCheckpointRetries: 5
    skipFailingRecords: false
    metrics:
      enabled: true
      level: SUMMARY

target:
    # ... usual target configuration ...
    
    pipeline:
      # .. usual processors ...
      
      - processorName: dynamoIndexingProcessor
        tables:
          - table1
          - table2
        credentials:
          accessKey: ${aws.credentials.accessKey}
          secretKey: ${aws.credentials.secretKey}
        region: ${aws.region}
        skipFailingRecords: ${aws.kinesis.skipFailingRecords}
```
