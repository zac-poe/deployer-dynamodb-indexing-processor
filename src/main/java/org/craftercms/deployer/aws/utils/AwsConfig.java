package org.craftercms.deployer.aws.utils;

import org.apache.commons.configuration2.Configuration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * Extracts AWS related values from a {@link Configuration} instance.
 *
 * @author joseross
 */
public abstract class AwsConfig {

    public static final String ACCESS_KEY_CONFIG_KEY = "credentials.accessKey";
    public static final String SECRET_KEY_CONFIG_KEY = "credentials.secretKey";
    public static final String REGION_CONFIG_KEY = "region";
    public static final String DYNAMODB_STREAM_CONFIG_KEY = "dynamoStream";

    public static final String WORKERS_CONFIG_KEY = "aws.kinesis.workers";
    public static final String WORKER_APP_NAME_CONFIG_KEY = "appName";
    public static final String WORKER_WORKER_ID_CONFIG_KEY = "workerId";
    public static final String WORKER_STREAM_CONFIG_KEY = "stream";

    public static boolean getUseDynamo(final Configuration config) {
        return config.getBoolean(DYNAMODB_STREAM_CONFIG_KEY, false);
    }

    public static AWSCredentialsProvider getCredentials(final Configuration config) {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
            config.getString(ACCESS_KEY_CONFIG_KEY),
            config.getString(SECRET_KEY_CONFIG_KEY)
        ));
    }

    public static String getRegionName(final Configuration config) {
        return config.getString(REGION_CONFIG_KEY);
    }

}
