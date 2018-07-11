# Crafter Deployer AWS Processors

## AWS Kinesis

Developed based on the sample project https://github.com/aws/aws-sdk-java/tree/master/src/samples/AmazonKinesis

## Example Configuration

### Target Context

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
       http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd">

    <bean id="kinesisFactory" class="org.craftercms.deployer.aws.kinesis.DeploymentKinesisProcessorFactory"/>

    <bean id="kinesisWorkerProcessor" class="org.craftercms.deployer.aws.processor.KinesisWorkerProcessor" parent="deploymentProcessor"/>

    <bean id="kinesisIndexingProcessor" class="org.craftercms.deployer.aws.processor.KinesisIndexingProcessor" parent="deploymentProcessor"/>
    
    <bean id="dynamoIndexingProcessor" class="org.craftercms.deployer.aws.processor.DynamoIndexingProcessor" parent="deploymentProcessor"/>

</beans>
```

### Target Configuration

```yaml
aws:
  region: us-west-1
  accessKey: ...
  secretKey: ...
target:
  env: aws
  siteName: test
  deployment:
    scheduling:
       enabled: false
    pipeline:
      - processorName: kinesisWorkerProcessor
        appName: crafter-deployer
        streamName: ...
        credentials:
          accessKey: ${aws.accessKey}
          secretKey: ${aws.secretKey}
        region: ${aws.region}
        workerId: crafter-deployer-localhost
        initialPosition: TRIM_HORIZON
        dynamoStream: true
      - processorName: kinesisIndexingProcessor
        dynamoStream: true
      - processorName: dynamoIndexingProcessor
        tables:
          - shows
          - clips
        credentials:
          accessKey: ${aws.accessKey}
          secretKey: ${aws.secretKey}
        region: ${aws.region}

```