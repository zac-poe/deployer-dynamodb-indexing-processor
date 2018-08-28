/*
 * Copyright (C) 2007-2018 Crafter Software Corporation.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
import org.craftercms.search.exception.SearchServerException;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import com.amazonaws.auth.AWSCredentialsProvider;
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
@SuppressWarnings({"unchecked", "rawtypes"})
public class DynamoIndexingProcessor extends AbstractMainDeploymentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DynamoIndexingProcessor.class);

    public static final String TABLES_CONFIG_KEY = "tables";

    /**
     * Name of the tables to scan.
     */
    protected List<String> tables;

    /**
     * Indicates if the processor should skip records that fail to index.
     */
    protected boolean continueOnError;

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
     * Configured region to connect to (provided in doInit)
     */
    private String region;

    /**
     * Configured credentials provider if using keys, otherwise will be null (provided in doInit)
     */
    private AWSCredentialsProvider credentialsProvider;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInit(final Configuration config) throws DeployerException {
        tables = config.getList(String.class, TABLES_CONFIG_KEY);

        continueOnError = AwsConfig.getContinueOnError(config);

        //save state for connecting at execution time
        region = AwsConfig.getRegionName(config);
        credentialsProvider = AwsConfig.getCredentials(config);

        logger.info("Connecting with " +
    		(credentialsProvider != null ? "access keys" : "IAM role default credentials provider"));
    }        

    /**
     * {@inheritDoc}
     */
    @Override
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {
    	//connect at execution time so that ProfileCredentialsProvider tokens do not expire
    	AmazonDynamoDB client = getClient();

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
                    } catch (SearchServerException e) {
                        logger.error("Search server is unavailable, will retry", e);
                        return false;
                    } catch (Exception e) {
                        logger.error("Processing of record failed", e);
                        return continueOnError;
                    }
                });
            }
        }
        Retry.untilTrue(() -> {
            try {
                searchService.commit(siteName);
                return true;
            } catch (SearchServerException e) {
                logger.error("Search server is unavailable, will retry", e);
                return false;
            } catch (SearchException e) {
                logger.error("Commit failed", e);
                return continueOnError;
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
    
    private AmazonDynamoDB getClient() {
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard()
                                                .withRegion(region);
        if(credentialsProvider != null) {
            builder.withCredentials(credentialsProvider);
        }
        
        return builder.build();
    }
}
