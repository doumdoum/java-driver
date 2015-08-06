/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.exceptions.DriverException;

class Exceptions {
    /** Helps dealing with execution exception when waiting on a future. */
    static RuntimeException toUnchecked(ExecutionException e) {
        if (e.getCause() instanceof Error) {
            throw ((Error)e.getCause());
        } else if (e.getCause() instanceof RuntimeException) {
            return ((RuntimeException)e.getCause());
        } else {
            return new DriverException("Unexpected error while initializing session", e.getCause());
        }
    }
}
