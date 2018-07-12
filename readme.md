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

    <bean id="kinesisWorkerManager" class="org.craftercms.deployer.aws.kinesis.KinesisWorkerManager"/>

    <bean id="kinesisIndexingProcessor" class="org.craftercms.deployer.aws.processor.KinesisIndexingProcessor" parent="deploymentProcessor"/>
    
    <bean id="dynamoIndexingProcessor" class="org.craftercms.deployer.aws.processor.DynamoIndexingProcessor" parent="deploymentProcessor"/>

</beans>
```

### Target Configuration

```yaml
aws:
  credentials:
    accessKey: ...
    secretKey: ...
  region: us-west-1
  kinesis:
    workers:
      - crafter-deployer-shows
      - crafter-deployer-shows-worker-1 
      - arn:aws:dynamodb:us-west-1:608786675545:table/people/stream/2018-07-12T14:25:35.434
      - crafter-deployer-clips
      - crafter-deployer-clips-worker-1
      - arn:aws:dynamodb:us-west-1:608786675545:table/pets/stream/2018-07-12T14:39:10.573
    initialPosition: TRIM_HORIZON
    useDynamo: true
target:
  env: aws
  siteName: test
  deployment:
    scheduling:
       enabled: false
    pipeline:
      - processorName: kinesisIndexingProcessor
        dynamoStream: ${aws.kinesis.useDynamo}
      - processorName: dynamoIndexingProcessor
        tables:
          - shows
          - clips
        credentials:
          accessKey: ${aws.credentials.accessKey}
          secretKey: ${aws.credentials.secretKey}
        region: ${aws.region}

```