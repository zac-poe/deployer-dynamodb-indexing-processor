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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

/**
 * General implementation of {@link IRecordProcessor} that handles checkpoints.
 *
 * @author joseross
 */
public abstract class AbstractKinesisRecordProcessor implements IRecordProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisRecordProcessor.class);

    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;

    /**
     * Maximum retries for processing a record set (negative value indicates to retry indefinitely until successful)
     */
    protected final int maxProcessingRetries;

    /**
     * Maximum retries for checkpointing (negative value indicates to retry indefinitely until successful)
     */
    protected final int maxCheckpointRetries;
    
    protected long nextCheckpointTimeInMillis;
    protected String kinesisShardId;
    
    public AbstractKinesisRecordProcessor(int maxProcessingRetries, int maxCheckpointRetries) {
    	this.maxProcessingRetries = maxProcessingRetries;
    	this.maxCheckpointRetries = maxCheckpointRetries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(final InitializationInput initializationInput) {
        kinesisShardId = initializationInput.getShardId();
        nextCheckpointTimeInMillis = getNextCheckpointTime();
        logger.info("Starting with shardId '{}'", kinesisShardId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        List<Record> records = processRecordsInput.getRecords();
        logger.info("Processing {} records from {}", records.size(), kinesisShardId);

        for(int i= 0; isInfinite(maxProcessingRetries) || i <= maxProcessingRetries; i++) {
	        if(tryProcessRecords(records)) {
	            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
	                checkpoint(processRecordsInput.getCheckpointer());
	                nextCheckpointTimeInMillis = getNextCheckpointTime();
	            }
	            handleProcessRecordsSuccess(processRecordsInput);
	            break;
	        } else {
	        	if(!isInfinite(maxProcessingRetries) && i >= maxProcessingRetries) {
	        		handleProcessRecordsFailure(processRecordsInput, i+1);
	        	} else {
		        	logger.warn("Unable to process kinesis stream records - attempt {}", i+1);
		        	sleep();
	        	}
	        }
        }
    }

    /**
     * Any desired handling following successfully processing records
     * @param processRecordsInput
     */
	protected void handleProcessRecordsSuccess(final ProcessRecordsInput processRecordsInput) {
		logger.debug("Successfully processed records");
	}

    /**
     * Any desired handling following failure to process records
     * @param processRecordsInput
     * @param attempt 1 based attempt counter
     */
	protected void handleProcessRecordsFailure(final ProcessRecordsInput processRecordsInput, final int attempt) {
		logger.error("Failed to process kinesis stream records after {} attempts. Record processor will bypass this record set!", attempt);
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(final ShutdownInput shutdownInput) {
        logger.info("Shutting down: {}", shutdownInput.getShutdownReason());
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
            	logger.info("No records left on shard, creating final checkpoint");
                checkpoint(shutdownInput.getCheckpointer());
            } catch (Exception e) {
                logger.error("Error creating checkpoint during shutdown", e);
            }
        }
    }

    /**
     *  Performs the actual checkpoint operation with retries.
     *
     * @param checkpointer Instance of {@link IRecordProcessorCheckpointer}
     */
    protected void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        logger.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; isInfinite(maxCheckpointRetries) || i <= maxCheckpointRetries; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                logger.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException | KinesisClientLibDependencyException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (!isInfinite(maxCheckpointRetries) && i >= maxCheckpointRetries) {
                	handleCheckpointFailure(checkpointer, i+1, e);
                } else {
                    logger.info("Transient issue when checkpointing - attempt " + (i+1)
                    		+ (isInfinite(maxCheckpointRetries) ? "" : " of " + maxCheckpointRetries), e);
                    sleep();
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
        }
    }

    /**
     * Any desired handling following failure to checkpoint
     * @param checkpointer
     * @param attempt 1 based attempt counter
     * @param cause throwable triggering checkpoint failure
     */
	protected void handleCheckpointFailure(final IRecordProcessorCheckpointer checkpointer, final int attempt, final Throwable cause) {
		logger.error("Checkpoint failed after " + attempt + "attempts.", cause);
	}

    /**
     * @return ms time of next checkpoint interval
     */
	protected long getNextCheckpointTime() {
		return System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
	}

    private boolean isInfinite(int val) {
    	return val < 0;
    }

    /**
     * Sleep between attempts for checkpoints or retrying processing of records
     */
	protected void sleep() {
		try {
		    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
		} catch (InterruptedException e) {
		    logger.debug("Interrupted sleep", e);
		}
	}

    /**
     * Performs the actual processing of the received records.
     * 
     * @param records List of records to process
     * @return true if processed successfully, false if processing should be re-attempted
     * @throws {@link RuntimeException} if thrown, will skip processing of these records!
     */
    protected abstract boolean tryProcessRecords(List<Record> records);
}
