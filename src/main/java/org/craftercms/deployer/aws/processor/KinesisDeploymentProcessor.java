package org.craftercms.deployer.aws.processor;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.craftercms.deployer.api.ChangeSet;
import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.ProcessorExecution;
import org.craftercms.deployer.api.exceptions.DeployerException;
import org.craftercms.deployer.aws.kinesis.DeploymentKinesisProcessor;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

/**
 * Implementation of {@link org.craftercms.deployer.api.DeploymentProcessor} that performs search index updates from
 * records received from an AWS Kinesis Data Stream or AWS DynamoDB Stream.  A processor instance can be configured
 * with the following YAML properties:
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
 *     <li><strong>dynamoStream:</strong> Indicates if the AWS DynamoDB Stream Adapter should be used, defaults to
 *     {@code false}</li>
 * </ul>
 *
 * @author joseross
 */
@SuppressWarnings("unchecked")
public class KinesisDeploymentProcessor extends AbstractMainDeploymentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KinesisDeploymentProcessor.class);

    public static final String APP_NAME_CONFIG_KEY = "appName";
    public static final String STREAM_NAME_CONFIG_KEY = "streamName";
    public static final String ACCESS_KEY_CONFIG_KEY = "credentials.accessKey";
    public static final String SECRET_KEY_CONFIG_KEY = "credentials.secretKey";
    public static final String WORKER_ID_CONFIG_KEY = "workerId";
    public static final String REGION_CONFIG_KEY = "region";
    public static final String INITIAL_POSITION_CONFIG_KEY = "initialPosition";
    public static final String DYNAMODB_STREAM_CONFIG_KEY = "dynamoStream";

    public static final String INITIAL_POSITION_DEFAULT_VALUE = "LATEST";

    protected Worker processorWorker;

    protected boolean useDynamo;

    protected ObjectMapper objectMapper;
    protected XmlMapper xmlMapper;
    protected CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    @Autowired
    protected IRecordProcessorFactory processorFactory;

    @Autowired
    protected SearchService searchService;

    /**
     * {@inheritDoc}
     */
    protected void doInit(final Configuration config) throws DeployerException {
        objectMapper = new ObjectMapper();
        xmlMapper = new XmlMapper();
        objectMapper.findAndRegisterModules();
        useDynamo = config.getBoolean(DYNAMODB_STREAM_CONFIG_KEY, false);
        AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
            config.getString(ACCESS_KEY_CONFIG_KEY),
            config.getString(SECRET_KEY_CONFIG_KEY)
        ));
        KinesisClientLibConfiguration configuration = new KinesisClientLibConfiguration(
            config.getString(APP_NAME_CONFIG_KEY),
            config.getString(STREAM_NAME_CONFIG_KEY),
            credentials,
            config.getString(WORKER_ID_CONFIG_KEY)
        );
        configuration.withRegionName(config.getString(REGION_CONFIG_KEY));
        configuration.withInitialPositionInStream(
            InitialPositionInStream.valueOf(config.getString(INITIAL_POSITION_CONFIG_KEY,
                INITIAL_POSITION_DEFAULT_VALUE)));
        Worker.Builder builder = new Worker.Builder().recordProcessorFactory(processorFactory).config(configuration);
        if(useDynamo) {
            builder.kinesisClient(new AmazonDynamoDBStreamsAdapterClient(credentials));
        }
        processorWorker = builder.build();
        new Thread(processorWorker).start();
    }

    /**
     * {@inheritDoc}
     */
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {

        List<Record> records = (List<Record>) deployment.getParam(DeploymentKinesisProcessor.RECORDS_PARAM_NAME);
        for(Record record : records) {
            try {
                Map map;
                if (useDynamo) {
                    RecordAdapter adapter = (RecordAdapter)record;
                    com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = adapter.getInternalObject();
                    if (!"REMOVE".equals(dynamoRecord.getEventName())) {
                        map = getDocFromDynamo(dynamoRecord);
                    } else {
                        throw new UnsupportedOperationException();
                    }
                } else {
                    map = getDocFromKinesis(record);
                }
                String id = (String) map.remove("id");
                String xml = xmlMapper.writeValueAsString(map);
                searchService.update(siteName, siteName, id, xml, true);
            } catch (Exception e) {
                throw new DeployerException("", e);
            }
        }
        searchService.commit(siteName);

        return null;
    }

    protected Map getDocFromKinesis(Record record) {
        //TODO: Get values from raw data, not neeeded for now...
        throw new UnsupportedOperationException();
    }

    protected Map getDocFromDynamo(com.amazonaws.services.dynamodbv2.model.Record record) {
        return ItemUtils.toItem(record.getDynamodb().getNewImage()).asMap();
    }

    /**
     * {@inheritDoc}
     */
    protected boolean failDeploymentOnProcessorFailure() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void destroy() throws DeployerException {
        processorWorker.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return deployment.getParam(DeploymentKinesisProcessor.RECORDS_PARAM_NAME) != null;
    }

}
