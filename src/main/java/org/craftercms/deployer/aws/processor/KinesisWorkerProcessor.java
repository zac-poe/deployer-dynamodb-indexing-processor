package org.craftercms.deployer.aws.processor;

import java.util.UUID;

import org.apache.commons.configuration2.Configuration;
import org.craftercms.deployer.api.ChangeSet;
import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.ProcessorExecution;
import org.craftercms.deployer.api.exceptions.DeployerException;
import org.craftercms.deployer.aws.utils.AwsConfig;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Implementation of {@link org.craftercms.deployer.api.DeploymentProcessor} that starts a {@link Worker} to
 * receive items from a AWS Kinesis Data Stream or DynamoDB Stream.  A processor instance can be configured with the
 * following YAML properties:
 *
 * <ul>
 *     <li><strong>appName:</strong> Name of the Kinesis application</li>
 *     <li><strong>streamName:</strong> Name of the Kinesis Data Stream or ARN of a DynamoDB Stream</li>
 *     <li>
 *         <strong>credentials:</strong>
 *         <ul>
 *             <li><strong>accessKey:</strong> AWS Access Key</li>
 *             <li><strong>secretKey:</strong> AWS Secret Key</li>
 *         </ul>
 *     </li>
 *     <li><strong>workerId:</strong> Id for the Kinesis Worker, must be unique</li>
 *     <li><strong>region:</strong> Name of the AWS Region</li>
 *     <li><strong>initialPosition:</strong> Value from {@link InitialPositionInStream}, default to {@code LATEST}</li>
 * </ul>
 *
 * @author joseross
 */
public class KinesisWorkerProcessor extends AbstractMainDeploymentProcessor {

    /**
     * Instance of {@link Worker}.
     */
    protected Worker processorWorker;

    /**
     * Instance of {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor}.
     */
    @Autowired
    protected IRecordProcessorFactory processorFactory;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInit(final Configuration config) throws DeployerException {
        AWSCredentialsProvider credentials = AwsConfig.getCredentials(config);
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration(
            AwsConfig.getAppName(config),
            AwsConfig.getStreamName(config),
            credentials,
            AwsConfig.getWorkerId(config) + "-" + UUID.randomUUID()
        );
        configuration.withRegionName(AwsConfig.getRegionName(config));
        configuration.withInitialPositionInStream(AwsConfig.getInitialPosition(config));
        Worker.Builder builder = new Worker.Builder().recordProcessorFactory(processorFactory).config(configuration);
        boolean useDynamo = AwsConfig.getUseDynamo(config);
        if(useDynamo) {
            builder.kinesisClient(new AmazonDynamoDBStreamsAdapterClient(credentials));
        }
        processorWorker = builder.build();
        new Thread(processorWorker).start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean failDeploymentOnProcessorFailure() {
        return true;
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws DeployerException {
        processorWorker.shutdown();
    }

}
