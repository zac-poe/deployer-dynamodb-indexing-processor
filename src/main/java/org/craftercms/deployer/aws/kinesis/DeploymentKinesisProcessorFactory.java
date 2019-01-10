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

import org.craftercms.deployer.api.DeploymentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Implementation of {@link IRecordProcessorFactory} to provide instances of {@link DeploymentKinesisProcessor}.
 *
 * @author joseross
 */
public class DeploymentKinesisProcessorFactory implements IRecordProcessorFactory {

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
     * Instance of the {@link DeploymentService}.
     */
    @Autowired
    protected DeploymentService deploymentService;

    /**
     * {@inheritDoc}
     */
    public IRecordProcessor createProcessor() {
        return new DeploymentKinesisProcessor(environment, siteName, true, deploymentService);
    }

}
