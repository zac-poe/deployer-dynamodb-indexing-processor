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

package org.craftercms.deployer.aws.utils;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Extracts AWS related values from a {@link Configuration} instance.
 *
 * @author joseross
 */
public abstract class AwsConfig {

    public static final String ACCESS_KEY_CONFIG_KEY = "credentials.accessKey";
    public static final String SECRET_KEY_CONFIG_KEY = "credentials.secretKey";
    public static final String REGION_CONFIG_KEY = "region";
    public static final String CONTINUE_ON_ERROR_CONFIG_KEY = "skipFailingRecords";

    public static final String AWS_SECTION = "aws";
    public static final String WORKERS_CONFIG_KEY = AWS_SECTION + ".kinesis.workers";
    public static final String WORKER_APP_NAME_CONFIG_KEY = "appName";
    public static final String WORKER_WORKER_ID_CONFIG_KEY = "workerId";
    public static final String WORKER_STREAM_CONFIG_KEY = "stream";
    public static final String TABLES_CONFIG_KEY = "tables";
    public static final String IS_DYNAMO_CONFIG_KEY = AWS_SECTION + ".kinesis.isDynamo";
    public static final String STREAM_INITIAL_POSITION_KEY = AWS_SECTION + ".kinesis.initialPosition";
    public static final String KINESIS_METRICS_ENABLED_KEY = AWS_SECTION + ".kinesis.metrics.enabled";
    public static final String KINESIS_METRICS_LEVEL_KEY = AWS_SECTION + ".kinesis.metrics.level";
    public static final String MAX_PROCESSING_RETRIES_KEY = AWS_SECTION + ".kinesis.maxProcessingRetries";
    public static final String MAX_CHECKPOINT_RETRIES_KEY = AWS_SECTION + ".kinesis.maxCheckpointRetries";

    public static final String CONTINUE_ON_ERROR_DEFAULT = "true";
    public static final String IS_DYNAMO_DEFAULT = "false";
    
    public static boolean getContinueOnError(final Configuration config) {
        return config.getBoolean(CONTINUE_ON_ERROR_CONFIG_KEY, Boolean.valueOf(CONTINUE_ON_ERROR_DEFAULT));
    }

    public static AWSCredentialsProvider getCredentials(final Configuration config) {
        if(StringUtils.isNotBlank(config.getString(ACCESS_KEY_CONFIG_KEY))) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getString(ACCESS_KEY_CONFIG_KEY),
                config.getString(SECRET_KEY_CONFIG_KEY)));
        } else {
            return null;
        }
    }

    public static String getRegionName(final Configuration config) {
        return config.getString(REGION_CONFIG_KEY);
    }

}
