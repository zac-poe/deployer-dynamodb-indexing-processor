package org.craftercms.deployer.aws.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class DeploymentKinesisProcessorFactoryTest {
	private DeploymentKinesisProcessorFactory target;

	@Before
	public void setup() {
		target = new DeploymentKinesisProcessorFactory();
	}

	@Test
	public void testDefaultProcessingRetriesAreDefined() {
		assertThat(target.maxProcessingRetries).isGreaterThan(0);
	}

	@Test
	public void testDefaultCheckpointRetriesAreDefined() {
		assertThat(target.maxCheckpointRetries).isGreaterThan(0);
	}

	@Test
	public void testConfigurationIsLogged() throws Exception {
		target.afterPropertiesSet();
		//verify log with attempts
		
		target.maxProcessingRetries = target.maxCheckpointRetries = -1;
		target.afterPropertiesSet();
		//veirfy log with indefinite
	}
}
