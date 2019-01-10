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
