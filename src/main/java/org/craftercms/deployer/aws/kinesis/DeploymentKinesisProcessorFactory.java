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
