/**
 * Copyright 2015 ParStream GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.parstream.adaptor.avro;

/**
 * This AvroAdaptorException indicates an exception occurred during the process
 * of decoding an Avro object to an Object[]. Details of the exception will be
 * indicated in the exception message.
 */
public class AvroAdaptorException extends Exception {

    private static final long serialVersionUID = 1;

    /**
     * Constructs an AvroAdaptorException with the specified detail message.
     * 
     * @param msg
     *            The detail message (which is saved for later retrieval by the
     *            Throwable.getMessage() method)
     */
    public AvroAdaptorException(String msg) {
        super(msg);
    }
}
