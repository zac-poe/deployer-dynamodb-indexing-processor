package org.craftercms.deployer.aws.processor;

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.craftercms.deployer.api.ChangeSet;
import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.ProcessorExecution;
import org.craftercms.deployer.api.exceptions.DeployerException;
import org.craftercms.deployer.aws.kinesis.DeploymentKinesisProcessor;
import org.craftercms.deployer.aws.utils.AwsConfig;
import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Implementation of {@link org.craftercms.deployer.api.DeploymentProcessor} that performs search update updates from
 * records received from an AWS Kinesis Data Stream or AWS DynamoDB Stream.  A processor instance can be configured
 * with the following YAML properties:
 *
 * <ul>
 *     <li><strong>dynamoStream:</strong> Indicates if the AWS DynamoDB Stream Adapter should be used, defaults to
 *     {@code false}</li>
 * </ul>
 *
 * @author joseross
 */
@SuppressWarnings("unchecked")
public class KinesisIndexingProcessor extends AbstractMainDeploymentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KinesisIndexingProcessor.class);

    protected boolean useDynamo;

    protected SearchHelper searchHelper = new SearchHelper();

    @Autowired
    protected SearchService searchService;

    /**
     * {@inheritDoc}
     */
    protected void doInit(final Configuration config) throws DeployerException {
        useDynamo = AwsConfig.getUseDynamo(config);
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
                        map = searchHelper.getDocFromDynamo(dynamoRecord);
                    } else {
                        throw new UnsupportedOperationException();
                    }
                } else {
                    map = searchHelper.getDocFromKinesis(record);
                }
                searchHelper.update(searchService, siteName, map);
            } catch (Exception e) {
                throw new DeployerException("Error processing record", e);
            }
        }
        searchService.commit(siteName);

        return null;
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

    }

    /**
     * {@inheritDoc}
     */
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return deployment.isRunning() && deployment.getParam(DeploymentKinesisProcessor.RECORDS_PARAM_NAME) != null;
    }

}
