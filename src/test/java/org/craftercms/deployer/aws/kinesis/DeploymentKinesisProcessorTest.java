package org.craftercms.deployer.aws.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.model.Record;

import java.util.ArrayList;

import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.Deployment.Status;
import org.craftercms.deployer.api.DeploymentService;
import org.craftercms.deployer.api.exceptions.DeploymentServiceException;
import org.craftercms.deployer.api.exceptions.TargetNotFoundException;
import org.junit.Before;
import org.junit.Test;

public class DeploymentKinesisProcessorTest {
	private DeploymentKinesisProcessor target;
	
	private DeploymentService mockDeploymentService;

	@Before
	public void setup() {
		mockDeploymentService = mock(DeploymentService.class);
		
		target = new DeploymentKinesisProcessor("", "", 0, 0, mockDeploymentService);
	}

	@Test
	public void testProcessingRecordsWaitsForDeploymentToFinish() throws Exception {
		when(mockDeploymentService.deployTarget(anyString(), anyString(), anyBoolean(), anyMap())).thenReturn(mock(Deployment.class));
		
		target.tryProcessRecords(new ArrayList<Record>());
		verify(mockDeploymentService).deployTarget(anyString(), anyString(), eq(true), anyMap());
	}

	@Test
	public void testProcessingRecordsIsSuccessfulWhenDeploymentIsSuccessful() throws Exception {
		Deployment result = mock(Deployment.class);
		when(result.getStatus()).thenReturn(Status.SUCCESS);
		when(mockDeploymentService.deployTarget(anyString(), anyString(), anyBoolean(), anyMap())).thenReturn(result);
		
		assertThat(target.tryProcessRecords(new ArrayList<Record>())).isTrue();
	}

	@Test
	public void testProcessingRecordsIsNotSuccessfulWhenDeploymentIsNotSuccessful() throws Exception {
		Deployment result = mock(Deployment.class);
		when(result.getStatus()).thenReturn(Status.FAILURE);
		when(mockDeploymentService.deployTarget(anyString(), anyString(), eq(true), anyMap())).thenReturn(result);
		
		assertThat(target.tryProcessRecords(new ArrayList<Record>())).isFalse();
	}

	@Test
	public void testProcessingRecordsIsNotSuccessfulWhenDeploymentIsNotFound() throws Exception {
		when(mockDeploymentService.deployTarget(anyString(), anyString(), eq(true), anyMap())).thenThrow(TargetNotFoundException.class);
		
		assertThat(target.tryProcessRecords(new ArrayList<Record>())).isFalse();
	}

	@Test
	public void testProcessingRecordsIsNotSuccessfulWhenDeploymentIsNotAccessible() throws Exception {
		when(mockDeploymentService.deployTarget(anyString(), anyString(), eq(true), anyMap())).thenThrow(DeploymentServiceException.class);
		
		assertThat(target.tryProcessRecords(new ArrayList<Record>())).isFalse();
	}
}
