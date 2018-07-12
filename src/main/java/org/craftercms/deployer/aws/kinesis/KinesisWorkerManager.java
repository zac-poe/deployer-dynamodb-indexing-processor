package org.craftercms.deployer.aws.kinesis;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Manages all required instances of {@link Worker}.
 *
 * @author joseross
 */
public class KinesisWorkerManager {

    @Value("${aws.region}")
    protected String region;

    @Value("${aws.kinesis.initialPosition:LATEST}")
    protected InitialPositionInStream initialPosition;

    @Value("${aws.kinesis.workers}")
    protected String[] workers;

    @Value("${aws.kinesis.useDynamo}")
    protected boolean useDynamo;

    @Value("${aws.credentials.accessKey}")
    protected String accessKey;

    @Value("${aws.credentials.secretKey}")
    protected String secretKey;

    protected AWSCredentialsProvider credentials;
    protected Worker.Builder builder;

    protected ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * Instance of {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor}.
     */
    @Autowired
    protected IRecordProcessorFactory processorFactory;

    /**
     * Creates and starts all {@link Worker} instances.
     */
    @PostConstruct
    public void init() {
        credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        for(int i = 0; i < workers.length; i += 3) {
            String appName = workers[i];
            String workerId = workers[i + 1];
            String stream = workers[i + 2];
            KinesisClientLibConfiguration configuration =
                new KinesisClientLibConfiguration(appName, stream, credentials, workerId);
            configuration.withRegionName(region);
            configuration.withInitialPositionInStream(initialPosition);
            builder = new Worker.Builder().recordProcessorFactory(processorFactory).config(configuration);
            if(useDynamo) {
                builder.kinesisClient(new AmazonDynamoDBStreamsAdapterClient(credentials));
            }
            executorService.submit(builder.build());
        }
    }

    /**
     * Request all {@link Worker}s to shutdown.
     */
    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }

}
