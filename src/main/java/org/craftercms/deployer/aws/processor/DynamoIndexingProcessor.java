package org.craftercms.deployer.aws.processor;

import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;
import org.craftercms.deployer.api.ChangeSet;
import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.ProcessorExecution;
import org.craftercms.deployer.api.exceptions.DeployerException;
import org.craftercms.deployer.aws.utils.AwsConfig;
import org.craftercms.deployer.aws.utils.Retry;
import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.deployer.impl.DeploymentConstants;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.craftercms.search.exception.SearchException;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Implementation of {@link AbstractMainDeploymentProcessor} that indexes records directly from
 * AWS DynamoDB tables.
 *
 * @author joseross
 */
@SuppressWarnings("unchecked")
public class DynamoIndexingProcessor extends AbstractMainDeploymentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DynamoIndexingProcessor.class);

    public static final String TABLES_CONFIG_KEY = "tables";

    /**
     * Name of the tables to scan.
     */
    protected List<String> tables;

    /**
     * AWS DynamoDB client.
     */
    protected AmazonDynamoDB client;

    /**
     * Helper to perform indexing.
     */
    protected SearchHelper searchHelper = new SearchHelper();

    /**
     * Current instance of {@link SearchService}.
     */
    @Autowired
    protected SearchService searchService;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInit(final Configuration config) throws DeployerException {
        tables = config.getList(String.class, TABLES_CONFIG_KEY);
        client = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(AwsConfig.getCredentials(config))
            .withRegion(AwsConfig.getRegionName(config))
            .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {

        for(String table : tables) {
            logger.info("Scanning table '{}'", table);
            ScanRequest request = new ScanRequest().withTableName(table);
            ScanResult result = client.scan(request);
            logger.info("Found {} items", result.getCount());
            for(Map map : result.getItems()){
                Retry.untilTrue(() -> {
                    try {
                        searchHelper.update(searchService, siteName, ItemUtils.toItem(map).asMap());
                        return true;
                    } catch (Exception e) {
                        logger.warn("Indexing failed, will retry", e);
                        return false;
                    }
                });
            }
        }
        Retry.untilTrue(() -> {
            try {
                searchService.commit(siteName);
                return true;
            } catch (SearchException e) {
                logger.warn("Commit failed, will retry", e);
                return false;
            }
        });

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
     */
    @Override
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return deployment.isRunning() &&
            deployment.getParam(DeploymentConstants.REPROCESS_ALL_FILES_PARAM_NAME) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws DeployerException {

    }

}
