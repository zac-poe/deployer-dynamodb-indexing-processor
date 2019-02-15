package org.craftercms.deployer.aws.kinesis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class AbstractKinesisRecordProcessorTest {
	protected AbstractKinesisRecordProcessor target;
	
	@Before
	public void setup() throws Exception {
		recreateTarget(0, 0);
	}
	
	protected void recreateTarget(int maxProcessingRetries, int maxCheckpointRetries) {
		target = spy(new TestKinesisRecordProcessor(maxProcessingRetries, maxCheckpointRetries));
	}

	@Test
	public void testProcessingIsSuccessfulWhenRecordsAreProcessedSuccessfully() throws Exception {
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(mock(IRecordProcessorCheckpointer.class)));
		
		verify(target, times(1)).tryProcessRecords(anyList());
		verify(target).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target, never()).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testProcessingSkipsRecordSetWhenRecordsAreNotProcessedSuccessfully() throws Exception {
		when(target.tryProcessRecords(anyList())).thenReturn(false);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(mock(IRecordProcessorCheckpointer.class)));
		
		verify(target, times(1)).tryProcessRecords(anyList());
		verify(target, never()).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testProcessingIsSuccessfulAfterRetries() throws Exception {
		int maxRetries = 3;
		recreateTarget(maxRetries, 0);
		
		doAnswer(i -> null).when(target).sleep();
		when(target.tryProcessRecords(anyList())).thenReturn(false)
			.thenReturn(false)
			.thenReturn(false)
			.thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(mock(IRecordProcessorCheckpointer.class)));
		
		verify(target, atMost(maxRetries + 1)).tryProcessRecords(anyList());
		verify(target, atMost(maxRetries)).sleep();
		verify(target).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target, never()).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testProcessingSkipsRecordSetWhenRecordsAfterMaxAttempts() throws Exception {
		int maxRetries = 3;
		recreateTarget(maxRetries, 0);

		when(target.tryProcessRecords(anyList())).thenReturn(false);
		doAnswer(i -> null).when(target).sleep();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(mock(IRecordProcessorCheckpointer.class)));
		
		verify(target, times(maxRetries + 1)).tryProcessRecords(anyList());
		verify(target, atMost(maxRetries)).sleep();
		verify(target, never()).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testCheckpointerHandlesShutdownState() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		doThrow(ShutdownException.class).when(checkpointer).checkpoint();
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer).checkpoint();
		verify(target, times(1)).tryProcessRecords(anyList());
		verify(target).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target, never()).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testCheckpointerHandlesInvalidDynamoState() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		doThrow(InvalidStateException.class).when(checkpointer).checkpoint();
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer).checkpoint();
		verify(target, times(1)).tryProcessRecords(anyList());
		verify(target).handleProcessRecordsSuccess(any(ProcessRecordsInput.class));
		verify(target, never()).handleProcessRecordsFailure(any(ProcessRecordsInput.class), anyInt());;
	}

	@Test
	public void testCheckpointerCheckpointsWhenRecordsAreProcessedSuccessfully() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer).checkpoint();
	}

	@Test
	public void testCheckpointerDoesNotCheckpointWhenRecordsAreNotProcessedSuccessfully() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(false);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, never()).checkpoint();
	}

	@Test
	public void testCheckpointerCheckpointsAfterProcessingRetries() throws Exception {
		int maxRetries = 3;
		recreateTarget(maxRetries, 0);
		
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		doAnswer(i -> null).when(target).sleep();
		when(target.tryProcessRecords(anyList())).thenReturn(false)
			.thenReturn(false)
			.thenReturn(false)
			.thenReturn(true);
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer).checkpoint();
	}

	@Test
	public void testCheckpointerDoesNotCheckpointAfterMaxProcessingAttempts() throws Exception {
		int maxRetries = 3;
		recreateTarget(maxRetries, 0);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(false);
		doAnswer(i -> null).when(target).sleep();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, never()).checkpoint();
	}

	@Test
	public void testCheckpointerCheckpointsAfterCheckpointingRetriesFromThrottling() throws Exception {
		int maxRetries = 3;
		recreateTarget(0, maxRetries);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doAnswer(i -> null).when(target).sleep();
		doThrow(ThrottlingException.class)
			.doThrow(ThrottlingException.class)
			.doThrow(ThrottlingException.class)
			.doNothing()
			.when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, atMost(maxRetries + 1)).checkpoint();
		verify(target, never()).handleCheckpointFailure(any(IRecordProcessorCheckpointer.class), anyInt(), any(Throwable.class));
	}

	@Test
	public void testCheckpointerDoesNotCheckpointAfterMaxCheckpointingAttemptsFromThrottling() throws Exception {
		int maxRetries = 3;
		recreateTarget(0, maxRetries);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doAnswer(i -> null).when(target).sleep();
		doThrow(ThrottlingException.class).when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, times(maxRetries + 1)).checkpoint();
		verify(target).handleCheckpointFailure(any(IRecordProcessorCheckpointer.class), anyInt(), any(Throwable.class));
	}

	@Test
	public void testCheckpointerCheckpointsAfterCheckpointingRetriesFromDependency() throws Exception {
		int maxRetries = 3;
		recreateTarget(0, maxRetries);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doAnswer(i -> null).when(target).sleep();
		doThrow(KinesisClientLibDependencyException.class)
			.doThrow(KinesisClientLibDependencyException.class)
			.doThrow(KinesisClientLibDependencyException.class)
			.doNothing()
			.when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, atMost(maxRetries + 1)).checkpoint();
		verify(target, never()).handleCheckpointFailure(any(IRecordProcessorCheckpointer.class), anyInt(), any(Throwable.class));
	}

	@Test
	public void testCheckpointerDoesNotCheckpointAfterMaxCheckpointingAttemptsFromDependency() throws Exception {
		int maxRetries = 3;
		recreateTarget(0, maxRetries);

		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		when(target.tryProcessRecords(anyList())).thenReturn(true);
		doAnswer(i -> null).when(target).sleep();
		doThrow(KinesisClientLibDependencyException.class).when(checkpointer).checkpoint();
		
		target.processRecords(new ProcessRecordsInput()
				.withRecords(new ArrayList<Record>())
				.withCheckpointer(checkpointer));
		
		verify(checkpointer, times(maxRetries + 1)).checkpoint();
		verify(target).handleCheckpointFailure(any(IRecordProcessorCheckpointer.class), anyInt(), any(Throwable.class));
	}

	@Test
	public void testCheckpointsOnShutdownTerminate() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		
		target.shutdown(new ShutdownInput()
				.withCheckpointer(checkpointer)
				.withShutdownReason(ShutdownReason.TERMINATE));
		
		verify(checkpointer).checkpoint();
	}

	@Test
	public void testDoesNotCheckpointsOnOtherShutdowns() throws Exception {
		IRecordProcessorCheckpointer checkpointer = mock(IRecordProcessorCheckpointer.class);
		
		target.shutdown(new ShutdownInput()
				.withCheckpointer(checkpointer)
				.withShutdownReason(ShutdownReason.REQUESTED));
		target.shutdown(new ShutdownInput()
				.withCheckpointer(checkpointer)
				.withShutdownReason(ShutdownReason.ZOMBIE));
		
		verify(checkpointer, never()).checkpoint();
	}

	private static class TestKinesisRecordProcessor extends AbstractKinesisRecordProcessor {
		public TestKinesisRecordProcessor(int maxProcessingRetries, int maxCheckpointRetries) {
			super(maxProcessingRetries, maxCheckpointRetries);
		}

		@Override
		protected boolean tryProcessRecords(List<Record> records) {
			return false;
		}
	}
}
