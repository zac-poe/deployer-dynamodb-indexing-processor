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
