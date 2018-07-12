package org.craftercms.deployer.aws.utils;

import java.util.function.Supplier;

/**
 * Utility class to perform retries and waits.
 *
 * @author joseross
 */
public abstract class Retry {

    public static final long DEFAULT_SLEEP = 5000;

    public static void untilTrue(Supplier<Boolean> action) {
        boolean successful = false;
        while(!successful) {
            successful = action.get();
            if(!successful) {
                sleep();
            }
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {

        }
    }

    public static void sleep() {
        sleep(DEFAULT_SLEEP);
    }

}
