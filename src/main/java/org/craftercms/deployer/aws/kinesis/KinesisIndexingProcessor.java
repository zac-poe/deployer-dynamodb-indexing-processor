/*
 * Copyright (C) 2007-2019 Crafter Software Corporation.
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

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.search.exception.SearchException;
import org.craftercms.search.exception.SearchServerException;
import org.craftercms.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor}
 * that performs indexing of kinesis {@link Record} data.
 *
 * @author joseross
 */
public class KinesisIndexingProcessor extends AbstractKinesisRecordProcessor {

	private static final Logger logger = LoggerFactory.getLogger(KinesisIndexingProcessor.class);

	/**
	 * Site to index data for.
	 */
	private final String siteName;

	/**
	 * Indicates if {@link Record} data is sourced from DynamoDb
	 */
	private boolean isDynamo;

	/**
	 * When true, processing will allow individual records to fail indexing (for example to bypass invalid data)
	 */
	private boolean continueOnError;

	/**
	 * Instance of search service
	 */
    @SuppressWarnings("rawtypes")
    private SearchService searchService;

	/**
	 * Instance of search helper to simplify indexing operations
	 */
    private SearchHelper searchHelper;

	public KinesisIndexingProcessor(final String siteName,
			final int maxProcessingRetries, final int maxCheckpointRetries,
			final boolean isDynamo, final boolean continueOnError,
			@SuppressWarnings("rawtypes") final SearchService searchService, final SearchHelper searchHelper) {
		super(maxProcessingRetries, maxCheckpointRetries);
		this.siteName = siteName;
		this.isDynamo = isDynamo;
		this.continueOnError = continueOnError;
		this.searchService = searchService;
		this.searchHelper = searchHelper;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean tryProcessRecords(final List<Record> records) {
		logger.debug("Trying to processing records for '{}'...", siteName);

		for (Record record : records) {
			try {
				if (isDynamo) {
					com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = ((RecordAdapter) record).getInternalObject();
					String event = dynamoRecord.getEventName();
					logger.debug("Processing record {}", event);
					switch (event) {
					case "REMOVE":
						searchHelper.delete(searchService, siteName, dynamoRecord);
						break;
					case "INSERT":
					case "MODIFY":
						searchHelper.update(searchService, siteName, searchHelper.getDocFromDynamo(dynamoRecord));
						break;
					default:
						logger.debug("No defined handling for event {}", event);
					}
				} else {
					searchHelper.update(searchService, siteName, searchHelper.getDocFromKinesis(record));
				}
			} catch (SearchServerException e) {
				logger.warn("Search server is presently unavailable to index data", e);
				return false;
			} catch (Exception e) {
				logger.error("Processing of record failed", e);
				if (!continueOnError) {
					return false;
				}
			}
		}
		
		try {
			logger.debug("Committing all changes for site '{}'", siteName);
			searchService.commit(siteName);
		} catch (SearchException e) {
			logger.warn("Search server is presently unavailable to commit data updates", e);
			return false;
		}

		return true;
	}
}
