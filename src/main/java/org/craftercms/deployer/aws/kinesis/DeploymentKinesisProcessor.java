/*
 * Copyright (C) 2007-2019 Crafter Software Corporation. All Rights Reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.DeploymentService;
import org.craftercms.deployer.api.exceptions.DeploymentServiceException;
import org.craftercms.deployer.api.exceptions.TargetNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Implementation of {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor} that
 * triggers a deployment on a given {@link org.craftercms.deployer.api.Target}.
 *
 * @author joseross
 */
public class DeploymentKinesisProcessor extends AbstractKinesisRecordProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DeploymentKinesisProcessor.class);

    /**
     * Name of the parameter used to pass the records to other processors.
     */
    public static final String RECORDS_PARAM_NAME = "records";

    /**
     * Environment to perform deployments.
     */
    protected String environment;

    /**
     * Site to perform deployments.
     */
    protected String siteName;

    /**
     * Indicates if the deployments should be executed synchronous.
     */
    protected boolean waitTillDone;

    /**
     * Instance of {@link DeploymentService}.
     */
    protected DeploymentService deploymentService;

    public DeploymentKinesisProcessor(final String environment, final String siteName, final boolean waitTillDone,
                                      final DeploymentService deploymentService) {
        this.environment = environment;
        this.siteName = siteName;
        this.waitTillDone = waitTillDone;
        this.deploymentService = deploymentService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean processRecords(final List<Record> records) throws TargetNotFoundException,
        DeploymentServiceException {
        logger.info("Triggering deployment for '{}-{}'", environment, siteName);
        Map<String, Object> params = new HashMap<>();
        params.put(RECORDS_PARAM_NAME, records);
        Deployment deployment = deploymentService.deployTarget(environment, siteName, waitTillDone, params);
        return deployment.getStatus() == Deployment.Status.SUCCESS;
    }

}
