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
package com.parstream.adaptor.avro.test;

import static org.junit.Assert.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Ignore;

import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamDate;
import com.parstream.driver.ParstreamShortDate;
import com.parstream.driver.ParstreamTime;
import com.parstream.driver.ParstreamTimestamp;

@Ignore
public class AdaptorTestUtils {

    /**
     * Used to easily define the types by whoever is writing a test.
     */
    @Ignore
    public enum Type {
        UINT8(257), UINT16(258), UINT32(260), UINT64(264), INT8(513), INT16(514), INT32(516), INT64(520), FLOAT(772), DOUBLE(
                776), VARSTRING(1025), SHORTDATE(1281), DATE(1282), TIME(1284), TIMESTAMP(1288), BITVECTOR8(2305), BLOB(
                2306);

        private int _type;

        private Type(int type) {
            _type = type;
        }

        public int getTypeCode() {
            return _type;
        }
    }

    /**
     * The constructors of ColumnInfo are private. During the testing of the
     * AvroDecoder, we need to supply it with a non-empty array of valid
     * ColumnInfo instances. The only way to instantiate objects with private
     * constructors is via reflection.
     * <p>
     * Integer.TYPE is used because the constructor of ColumnInfo uses the
     * primitive datatype int. If the constructor uses the Integer class though,
     * Integer.class should be used instead.
     */
    static ColumnInfo constructColumnInfo(String name, AdaptorTestUtils.Type type, int singularity, int length) {
        ColumnInfo colInfo = null;
        Constructor<ColumnInfo> constructor;
        try {
            constructor = ColumnInfo.class.getDeclaredConstructor(String.class, Integer.TYPE, Integer.TYPE,
                    Integer.TYPE, Integer.TYPE);
            constructor.setAccessible(true);
            colInfo = constructor.newInstance(name, type.getTypeCode(), singularity, length, 0);
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            fail("cannot access or use ColumnInfo constructor");
        }
        return colInfo;
    }

    /**
     * ParstreamShortDate, ParstreamDate, ParstreamTime, ParstreamTimestamp are
     * classes defined to store the internal values of these Parstream datatyes.
     * These classes have no getters to the values stored in them. For the time
     * being, they also do not implement the comparable interface.
     * <p>
     * This aim of this method is to provide access to the privately stored
     * value in the object, and compare it for equality (required for decoder
     * testing).
     */
    static <T> boolean isParstreamDateObjectIdentical(Class<T> c, T d1, T d2) throws Exception {
        Method valueMethod = c.getDeclaredMethod("getInternalValue");
        valueMethod.setAccessible(true);

        Object res1 = valueMethod.invoke(d1);
        Object res2 = valueMethod.invoke(d2);

        if (c.equals(ParstreamShortDate.class)) {
            return ((Integer) res1).equals((Integer) res2);
        } else if (c.equals(ParstreamDate.class) || c.equals(ParstreamTime.class) || c.equals(ParstreamTimestamp.class)) {
            return ((Long) res1).equals((Long) res2);
        }
        // this will throw a AssertionError (runtime exception)
        fail("Internal error");
        return false;
    }
}
