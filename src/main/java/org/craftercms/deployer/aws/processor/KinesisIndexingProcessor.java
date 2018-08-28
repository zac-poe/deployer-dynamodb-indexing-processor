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
import org.craftercms.deployer.aws.kinesis.DeploymentKinesisProcessor;
import org.craftercms.deployer.aws.kinesis.KinesisWorkerManager;
import org.craftercms.deployer.aws.utils.AwsConfig;
import org.craftercms.deployer.aws.utils.Retry;
import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.craftercms.search.exception.SearchServerException;
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
 *     <li><strong>continueOnError:</strong> Indicates if the processor should skip records that fail to be indexed,
 *     defaults to {@code true}</li>
 * </ul>
 *
 * @author joseross
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class KinesisIndexingProcessor extends AbstractMainDeploymentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(KinesisIndexingProcessor.class);

    protected boolean useDynamo;

    protected boolean continueOnError;

    protected SearchHelper searchHelper = new SearchHelper();

    @Autowired
    protected SearchService searchService;

    @Autowired
    protected KinesisWorkerManager workerManager;

    /**
     * {@inheritDoc}
     */
    protected void doInit(final Configuration config) throws DeployerException {
        useDynamo = AwsConfig.getUseDynamo(config);
        continueOnError = AwsConfig.getContinueOnError(config);
    }

    /**
     * {@inheritDoc}
     */
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {

        List<Record> records = (List<Record>) deployment.getParam(DeploymentKinesisProcessor.RECORDS_PARAM_NAME);
        for(Record record : records) {
            Retry.untilTrue(() -> {
                try {
                    if (useDynamo) {
                        RecordAdapter adapter = (RecordAdapter)record;
                        com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = adapter.getInternalObject();
                        switch (dynamoRecord.getEventName()) {
                            case "REMOVE":
                                searchHelper.delete(searchService, siteName, dynamoRecord);
                                break;
                            case "INSERT":
                            case "MODIFY":
                                Map map;
                                map = searchHelper.getDocFromDynamo(dynamoRecord);
                                logger.debug("Indexing doc with id '{}'", map.get("id"));
                                searchHelper.update(searchService, siteName, map);
                                break;
                        }
                        return true;
                    } else {
                        searchHelper.update(searchService, siteName, searchHelper.getDocFromKinesis(record));
                        return true;
                    }
                } catch (SearchServerException e) {
                    logger.error("Search server is unavailable, will retry", e);
                    return false;
                } catch (Exception e) {
                    logger.error("Processing of record failed", e);
                    return continueOnError;
                }
            });
        }
        Retry.untilTrue(() -> {
           try {
               logger.debug("Committing all changes for site '{}'", siteName);
               searchService.commit(siteName);
               return true;
           } catch (SearchServerException e) {
               logger.error("Search server is unavailable, will retry", e);
               return false;
           } catch (Exception e) {
               logger.error("Commit failed", e);
               return continueOnError;
           }
        });

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
        workerManager.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return deployment.isRunning() && deployment.getParam(DeploymentKinesisProcessor.RECORDS_PARAM_NAME) != null;
    }

}
