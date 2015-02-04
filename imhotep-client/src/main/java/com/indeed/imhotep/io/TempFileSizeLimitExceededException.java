/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indeed.imhotep.io;

/**
 * @author vladimir
 */

public class TempFileSizeLimitExceededException extends RuntimeException {
    final static String MESSAGE = "The request tried to write more data to disk than allowed by the TempFileSizeLimit parameter";

    public TempFileSizeLimitExceededException() {
        super(MESSAGE);
    }

    public TempFileSizeLimitExceededException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
