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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.craftercms.deployer.aws.utils.AwsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

/**
 * Manages all required instances of {@link Worker}.
 *
 * @author joseross
 */
@SuppressWarnings("rawtypes")
public class KinesisWorkerManager {

    @Value("${aws.region}")
    protected String region;

    @Value("${aws.kinesis.initialPosition:LATEST}")
    protected InitialPositionInStream initialPosition;

    @Value("${aws.kinesis.useDynamo}")
    protected boolean useDynamo;

    @Value("${aws.kinesis.metrics.enabled:false}")
    protected boolean useMetrics;

    @Value("${aws.kinesis.metrics.level:NONE}")
    protected MetricsLevel metricsLevel;

    @Value("${aws.credentials.accessKey:}")
    protected String accessKey;

    @Value("${aws.credentials.secretKey:}")
    protected String secretKey;

    protected Worker.Builder builder;

    protected ExecutorService executorService = Executors.newCachedThreadPool();

    @Autowired
    protected HierarchicalConfiguration targetConfig;

    /**
     * Instance of {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor}.
     */
    @Autowired
    protected IRecordProcessorFactory processorFactory;

    /**
     * Creates and starts all {@link Worker} instances.
     */
    @PostConstruct
    @SuppressWarnings("unchecked")
    public void init() {
        AWSCredentialsProvider provider;
        if(StringUtils.isEmpty(accessKey)) {
            provider = DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }
        List<Configuration> workers = targetConfig.configurationsAt(AwsConfig.WORKERS_CONFIG_KEY);
        workers.forEach(worker ->{
            String appName = worker.getString(AwsConfig.WORKER_APP_NAME_CONFIG_KEY);
            String workerId = worker.getString(AwsConfig.WORKER_WORKER_ID_CONFIG_KEY);
            String stream = worker.getString(AwsConfig.WORKER_STREAM_CONFIG_KEY);
            KinesisClientLibConfiguration configuration =
                new KinesisClientLibConfiguration(appName, stream, provider, workerId);
            configuration.withRegionName(region);
            configuration.withInitialPositionInStream(initialPosition);
            if(useMetrics) {
                configuration.withMetricsLevel(metricsLevel);
            }
            builder = new Worker.Builder().recordProcessorFactory(processorFactory).config(configuration);
            if(!useMetrics) {
                builder.metricsFactory(new NullMetricsFactory());
            }
            if(useDynamo) {
                builder.kinesisClient(new AmazonDynamoDBStreamsAdapterClient(provider));
            }
            executorService.submit(builder.build());
        });
    }

    /**
     * Request all {@link Worker}s to shutdown.
     */
    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
    }

}
