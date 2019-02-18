package org.craftercms.deployer.aws.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.model.Record;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.craftercms.deployer.aws.utils.SearchHelper;
import org.craftercms.search.exception.SearchException;
import org.craftercms.search.exception.SearchServerException;
import org.craftercms.search.service.SearchService;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({"rawtypes", "unchecked"})
public class KinesisIndexingProcessorTest {
	private KinesisIndexingProcessor target;
	
	private SearchService mockSearchService;
    private SearchHelper mockSearchHelper;

	@Before
	public void setup() {
		mockSearchService = mock(SearchService.class);
		mockSearchHelper = mock(SearchHelper.class);
		target = null; //prevent forgetting to create test specific settings
	}
	
	public void createTarget(boolean isDynamo, boolean continueOnError) {
		target = new KinesisIndexingProcessor("", 0, 0, isDynamo, continueOnError, mockSearchService, mockSearchHelper);
	}

	@Test
	public void testProcessingRecordsIsSuccessfulForKinesisUpdate() throws Exception {
		createTarget(false, false);

		Map record = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(record);
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isTrue();
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record));
	}

	@Test
	public void testProcessingRecordsIsSuccessfulForDynamoUpdate() throws Exception {
		createTarget(true, false);

		RecordAdapter record = mock(RecordAdapter.class);
		Map doc = mock(Map.class);
		com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = mock(com.amazonaws.services.dynamodbv2.model.Record.class);
		when(record.getInternalObject()).thenReturn(dynamoRecord);
		when(mockSearchHelper.getDocFromDynamo(dynamoRecord)).thenReturn(doc);
		when(dynamoRecord.getEventName()).thenReturn("MODIFY");
		
		assertThat(target.tryProcessRecords(Arrays.asList(record))).isTrue();
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(doc));
	}

	@Test
	public void testProcessingRecordsIsSuccessfulForDynamoInsert() throws Exception {
		createTarget(true, false);

		RecordAdapter record = mock(RecordAdapter.class);
		Map doc = mock(Map.class);
		com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = mock(com.amazonaws.services.dynamodbv2.model.Record.class);
		when(record.getInternalObject()).thenReturn(dynamoRecord);
		when(mockSearchHelper.getDocFromDynamo(dynamoRecord)).thenReturn(doc);
		when(dynamoRecord.getEventName()).thenReturn("INSERT");
		
		assertThat(target.tryProcessRecords(Arrays.asList(record))).isTrue();
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(doc));
	}

	@Test
	public void testProcessingRecordsIsSuccessfulForDynamoDelete() throws Exception {
		createTarget(true, false);

		RecordAdapter record = mock(RecordAdapter.class);
		com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = mock(com.amazonaws.services.dynamodbv2.model.Record.class);
		when(record.getInternalObject()).thenReturn(dynamoRecord);
		when(dynamoRecord.getEventName()).thenReturn("REMOVE");
		
		assertThat(target.tryProcessRecords(Arrays.asList(record))).isTrue();
		verify(mockSearchHelper).delete(eq(mockSearchService), anyString(), eq(dynamoRecord));
	}

	@Test
	public void testProcessingMultipleRecords() throws Exception {
		createTarget(false, false);

		Map record1 = mock(Map.class), record2 = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class)))
			.thenReturn(record1)
			.thenReturn(record2);
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class), mock(Record.class)))).isTrue();
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record1));
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record2));
	}

	@Test
	public void testProcessingRecordsIsNotSuccessfulWhenSearchServiceIsNotAvailable() throws Exception {
		createTarget(false, false);

		Map record = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(record);
		doThrow(SearchServerException.class).when(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record));
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isFalse();
	}

	@Test
	public void testProcessingRecordsIsSuccessfulWhenUpdateFailsButConfiguredToSkipFailures() throws Exception {
		createTarget(false, true);

		Map record = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(record);
		doThrow(SearchException.class).when(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record));
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isTrue();
	}

	@Test
	public void testProcessingRecordsIsNotSuccessfulWhenUpdateFailsAndNotConfiguredToSkipFailures() throws Exception {
		createTarget(false, false);

		Map record = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(record);
		doThrow(SearchException.class).when(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record));
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isFalse();
	}

	@Test
	public void testProcessingSubsequentRecordsWhenAnUpdateFailsButConfiguredToSkipFailures() throws Exception {
		createTarget(false, true);

		Map record1 = mock(Map.class), record2 = mock(Map.class);
		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(record1).thenReturn(record2);
		doThrow(SearchException.class).when(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record1));
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class), mock(Record.class)))).isTrue();
		verify(mockSearchHelper).update(eq(mockSearchService), anyString(), eq(record2));
	}

	@Test
	public void testProcessingRecordsCommitsUpdate() throws Exception {
		createTarget(false, false);

		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(new HashMap<>());
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isTrue();
		verify(mockSearchService).commit(anyString());
	}

	@Test
	public void testProcessingRecordsFailsOnCommitFailure() throws Exception {
		createTarget(false, false);

		when(mockSearchHelper.getDocFromKinesis(any(Record.class))).thenReturn(new HashMap<>());
		doThrow(SearchException.class).when(mockSearchService).commit(anyString());
		
		assertThat(target.tryProcessRecords(Arrays.asList(mock(Record.class)))).isFalse();
	}
}
