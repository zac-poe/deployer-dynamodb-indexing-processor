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

package org.craftercms.deployer.aws.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.model.Record;

import org.craftercms.deployer.aws.utils.AwsConfig;
import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Implementation of {@link IRecordProcessorFactory} to provide instances of {@link KinesisIndexingProcessor}.
 *
 * @author joseross
 */
public class KinesisIndexingProcessorFactory implements IRecordProcessorFactory, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(KinesisIndexingProcessorFactory.class);

    /**
     * Site to index to.
     */
    @Value("${target.siteName}")
    private String siteName;

    /**
     * Maximum retries for processing a record set (negative value indicates to retry indefinitely until successful)
     */
    @Value("${" + AwsConfig.MAX_PROCESSING_RETRIES_KEY + ":3}")
    private int maxProcessingRetries;

    /**
     * Maximum retries for checkpointing (negative value indicates to retry indefinitely until successful)
     */
    @Value("${" + AwsConfig.MAX_CHECKPOINT_RETRIES_KEY + ":10}")
    private int maxCheckpointRetries;

	/**
	 * Indicates if {@link Record} data is sourced from DynamoDb
	 */
    @Value("${" + AwsConfig.IS_DYNAMO_CONFIG_KEY + ":" + AwsConfig.IS_DYNAMO_DEFAULT + "}")
    private boolean isDynamo;

	/**
	 * When true, processing will allow individual records to fail indexing (for example to bypass invalid data)
	 */
	@Value("${" + AwsConfig.AWS_SECTION + "." + AwsConfig.CONTINUE_ON_ERROR_CONFIG_KEY + ":" + AwsConfig.CONTINUE_ON_ERROR_DEFAULT + "}")
	private boolean skipFailingRecords;

    /**
     * Instance of the {@link SearchService}.
     */
    @SuppressWarnings("rawtypes")
	@Autowired
	private SearchService searchService; 
    
    private SearchHelper searchHelper = new SearchHelper();
    
    /**
     * {@inheritDoc}
     */
    public IRecordProcessor createProcessor() {
        return new KinesisIndexingProcessor(siteName, maxProcessingRetries, maxCheckpointRetries,
        		isDynamo, skipFailingRecords,
        		searchService, searchHelper);
    }

	@Override
	public void afterPropertiesSet() throws Exception {
        logger.info("Kinesis record processors for site {} will be created using: processing max retries: {}, checkpoint max retries: {}, using dynamo: {}, skip failed records: {}",
        		siteName,
        		getRetryDescription(maxProcessingRetries),
        		getRetryDescription(maxCheckpointRetries),
        		isDynamo,
        		skipFailingRecords);
    }

	private String getRetryDescription(int attempts) {
		return AbstractKinesisRecordProcessor.isInfiniteAttempts(attempts) ? "indefinite" : (attempts + " attempts");
	}
}
