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

import org.craftercms.deployer.api.DeploymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Implementation of {@link IRecordProcessorFactory} to provide instances of {@link DeploymentKinesisProcessor}.
 *
 * @author joseross
 */
public class DeploymentKinesisProcessorFactory implements IRecordProcessorFactory, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(DeploymentKinesisProcessorFactory.class);

    /**
     * Environment to perform deployments.
     */
    @Value("${target.env}")
    protected String environment;

    /**
     * Site to perform deployments.
     */
    @Value("${target.siteName}")
    protected String siteName;

    /**
     * Maximum retries for processing a record set (negative value indicates to retry indefinitely until successful)
     */
    protected int maxProcessingRetries = 3;

    /**
     * Maximum retries for checkpointing (negative value indicates to retry indefinitely until successful)
     */
    protected int maxCheckpointRetries = 10;

    /**
     * Instance of the {@link DeploymentService}.
     */
    @Autowired
    protected DeploymentService deploymentService;

    /**
     * {@inheritDoc}
     */
    public IRecordProcessor createProcessor() {
        return new DeploymentKinesisProcessor(environment, siteName, maxProcessingRetries, maxCheckpointRetries, deploymentService);
    }

	@Override
	public void afterPropertiesSet() throws Exception {
        logger.info("Kinesis record processors will be created using: processing max retries: {}, checkpoint max retries: {}",
        		getRetryDescription(maxProcessingRetries),
        		getRetryDescription(maxCheckpointRetries));
    }

	private String getRetryDescription(int attempts) {
		return AbstractKinesisRecordProcessor.isInfiniteAttempts(attempts) ? "indefinite" : (attempts + " attempts");
	}
}
