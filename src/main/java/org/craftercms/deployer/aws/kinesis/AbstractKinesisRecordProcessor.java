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
    private static final int NUM_RETRIES = 10;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;

    protected long nextCheckpointTimeInMillis;
    protected String kinesisShardId;
    protected boolean failed = false;

    /**
     * {@inheritDoc}
     */
    public void initialize(final InitializationInput initializationInput) {
        kinesisShardId = initializationInput.getShardId();
        logger.info("Starting with shardId '{}'", kinesisShardId);
    }

    /**
     * {@inheritDoc}
     */
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        List<Record> records = processRecordsInput.getRecords();
        logger.info("Processing {} records from {}", records.size(), kinesisShardId);

        try {
            if(processRecords(records)) {
                if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                    checkpoint(processRecordsInput.getCheckpointer());
                    nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
                }
            } else {
                failed = true;
            }
        } catch (Exception e) {
            logger.error("Error processing records", e);
            failed = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void shutdown(final ShutdownInput shutdownInput) {
        logger.info("Shutting down");
        if (!failed && shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            }
            catch (Exception e) {
                logger.error("Error shutting down", e);
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
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                logger.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                        + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted sleep", e);
            }
        }
    }

    /**
     * Performs the actual processing of the received records.
     *
     * @param records List of records to process
     */
    public abstract boolean processRecords(List<Record> records) throws Exception;

}
