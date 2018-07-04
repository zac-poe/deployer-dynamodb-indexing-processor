package org.craftercms.deployer.aws.kinesis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.craftercms.deployer.api.DeploymentService;
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
    public void processRecords(final List<Record> records) {
        Map<String, Object> params = new HashMap<>();
        params.put(RECORDS_PARAM_NAME, records);
        try {
            deploymentService.deployTarget(environment, siteName, waitTillDone, params);
        } catch (Exception e) {
            logger.error("Error deploying records", e);
        }
    }

}
