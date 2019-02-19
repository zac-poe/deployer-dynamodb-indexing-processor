package org.craftercms.deployer.aws.kinesis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;

import java.util.ArrayList;

import org.junit.Test;

/**
 * Exists for manual runs only.
 * These tests must be forcibly killed, since loops will run infinitely!
 */
public class AbstractKinesisRecordProcessorDriver extends AbstractKinesisRecordProcessorTest {

	/**
	 * throttles infinite loop
	 * @param isProcessing true for processing, else for checkpointing
	 */
	void infiniteSetup(boolean isProcessing) {
		recreateTarget(isProcessing ? -1 : 0, isProcessing ? 0 : -1);

		doAnswer(i -> {
			Thread.sleep(50);
			return null;
		}).when(target).sleep();
	}
	
	@Test
	public void testProcessingGoesInfiniteWhenDefined() throws Exception {
		infiniteSetup(true);
		
		when(target.tryProcessRecords(anyList())).thenReturn(false);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(mock(IRecordProcessorCheckpointer.class)));
		
		//verify is running indefinitely
	}
	
	@Test
	public void testCheckpointerDoesNotCheckpointAfterMaxCheckpointingAttemptsFromThrottling() throws Exception {
		infiniteSetup(false);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		//verify is running indefinitely
	}

	@Test
	public void testCheckpointerDoesNotCheckpointAfterMaxCheckpointingAttemptsFromDependency() throws Exception {
		infiniteSetup(false);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doThrow(KinesisClientLibDependencyException.class).when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		//verify is running indefinitely
	}
}
