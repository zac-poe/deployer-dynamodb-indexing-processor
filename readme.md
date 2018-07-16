# Crafter Deployer AWS Processors

## AWS Kinesis

Developed based on the [sample project](https://github.com/aws/aws-sdk-java/tree/master/src/samples/AmazonKinesis)

## Usage

Build de project by running `mvn clean package` and then copy the `target/deployer-aws-processors-{VERSION}.jar` file 
to `$INSTALL_DIR/bin/crafter-deployer/lib`.

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

    <bean id="kinesisFactory" class="org.craftercms.deployer.aws.kinesis.DeploymentKinesisProcessorFactory"/>

    <bean id="kinesisWorkerManager" class="org.craftercms.deployer.aws.kinesis.KinesisWorkerManager"/>

    <bean id="kinesisIndexingProcessor" class="org.craftercms.deployer.aws.processor.KinesisIndexingProcessor" parent="deploymentProcessor"/>
    
    <bean id="dynamoIndexingProcessor" class="org.craftercms.deployer.aws.processor.DynamoIndexingProcessor" parent="deploymentProcessor"/>

</beans>
```

### Target Configuration

At least one worker must be defined under `aws.kinesis.workers`. The Kinesis Client Library configuration must follow
these restrictions:

- `workerId` must be unique for a given `appName`
- all workers under the same `appName` use the same `stream`
- because `appName` will be used internally as a DynamoDB table it must be unique for the given `region`
- the credentials used must have the required permissions for both Kinesis & DynamoDB

`aws.intialPosition` is only needed if the processor should handle all pending records when it starts, the default
behaviour is to only receive new ones after it is started.

`aws.credentials` & `dynamoIndexingProcessor.credentials` are both optional, if they are not provided the default
credential provider chain will be used. [More info](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)

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
    useDynamo: true

target:
    # ... usual target configuration ...
    
    pipeline:
      # .. usual processors ...
      
      - processorName: kinesisIndexingProcessor
        dynamoStream: ${aws.kinesis.useDynamo}
      - processorName: dynamoIndexingProcessor
        tables:
          - table1
          - table2
        credentials:
          accessKey: ${aws.credentials.accessKey}
          secretKey: ${aws.credentials.secretKey}
        region: ${aws.region}

```