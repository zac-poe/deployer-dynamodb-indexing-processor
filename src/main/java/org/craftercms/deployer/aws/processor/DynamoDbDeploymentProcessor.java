package org.craftercms.deployer.aws.processor;

import org.apache.commons.configuration2.Configuration;
import org.craftercms.deployer.api.ChangeSet;
import org.craftercms.deployer.api.Deployment;
import org.craftercms.deployer.api.ProcessorExecution;
import org.craftercms.deployer.api.exceptions.DeployerException;
import org.craftercms.deployer.impl.DeploymentConstants;
import org.craftercms.deployer.impl.processors.AbstractMainDeploymentProcessor;
import org.craftercms.search.service.SearchService;
import org.springframework.beans.factory.annotation.Autowired;

public class DynamoDbDeploymentProcessor extends AbstractMainDeploymentProcessor {

    @Autowired
    protected SearchService searchService;

    @Override
    protected void doInit(final Configuration config) throws DeployerException {

    }

    @Override
    protected ChangeSet doExecute(final Deployment deployment, final ProcessorExecution execution, final ChangeSet
        filteredChangeSet) throws DeployerException {

        // delete all?

        // fetch from dynamodb

        // update all

        return null;
    }

    @Override
    protected boolean failDeploymentOnProcessorFailure() {
        return true;
    }

    @Override
    protected boolean shouldExecute(final Deployment deployment, final ChangeSet filteredChangeSet) {
        return deployment.getParam(DeploymentConstants.REPROCESS_ALL_FILES_PARAM_NAME) != null;
    }

    @Override
    public void destroy() throws DeployerException {

    }

}
