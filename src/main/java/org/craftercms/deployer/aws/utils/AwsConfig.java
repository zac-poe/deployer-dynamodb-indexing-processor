package org.craftercms.deployer.aws.utils;

import org.apache.commons.configuration2.Configuration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * Extracts AWS related values from a {@link Configuration} instance.
 *
 * @author joseross
 */
public abstract class AwsConfig {

    public static final String APP_NAME_CONFIG_KEY = "appName";
    public static final String STREAM_NAME_CONFIG_KEY = "streamName";
    public static final String ACCESS_KEY_CONFIG_KEY = "credentials.accessKey";
    public static final String SECRET_KEY_CONFIG_KEY = "credentials.secretKey";
    public static final String WORKER_ID_CONFIG_KEY = "workerId";
    public static final String REGION_CONFIG_KEY = "region";
    public static final String INITIAL_POSITION_CONFIG_KEY = "initialPosition";
    public static final String DYNAMODB_STREAM_CONFIG_KEY = "dynamoStream";

    public static final String INITIAL_POSITION_DEFAULT_VALUE = "LATEST";

    public static boolean getUseDynamo(final Configuration config) {
        return config.getBoolean(DYNAMODB_STREAM_CONFIG_KEY, false);
    }

    public static AWSCredentialsProvider getCredentials(final Configuration config) {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
            config.getString(ACCESS_KEY_CONFIG_KEY),
            config.getString(SECRET_KEY_CONFIG_KEY)
        ));
    }

    public static String getAppName(final Configuration config) {
        return config.getString(APP_NAME_CONFIG_KEY);
    }

    public static String getStreamName(final Configuration config) {
        return config.getString(STREAM_NAME_CONFIG_KEY);
    }

    public static String getWorkerId(final Configuration config) {
        return config.getString(WORKER_ID_CONFIG_KEY);
    }

    public static String getRegionName(final Configuration config) {
        return config.getString(REGION_CONFIG_KEY);
    }

    public static InitialPositionInStream getInitialPosition(final Configuration config) {
        return InitialPositionInStream.valueOf(
            config.getString(INITIAL_POSITION_CONFIG_KEY, INITIAL_POSITION_DEFAULT_VALUE));
    }

}
