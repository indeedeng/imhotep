/*
 * Copyright (C) 2018 Indeed Inc.
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

package com.indeed.imhotep;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */
public class DynamicIndexSubshardDirnameUtilTest {

    @Test
    public void testGetShardId() {
        final String shardName = DynamicIndexSubshardDirnameUtil.getShardId(TimeUnit.HOURS.toMillis(6), TimeUnit.HOURS.toMillis(7), 1, 4);
        assertEquals("dindex19700101.00-19700101.01.1.4", shardName);
    }

    @Test
    public void testParse() {
        final String shardName = "dindex19700101.00-19700101.01.1.4.1234.5678";
        final Optional<DynamicIndexSubshardDirnameUtil.DynamicIndexShardInfo> shardInfoOrEmpty = DynamicIndexSubshardDirnameUtil.tryParse(shardName);
        assertTrue(shardInfoOrEmpty.isPresent());
        final DynamicIndexSubshardDirnameUtil.DynamicIndexShardInfo dynamicIndexShardInfo = shardInfoOrEmpty.get();
        assertEquals(shardName, dynamicIndexShardInfo.getName());
        assertEquals("dindex19700101.00-19700101.01.1.4", dynamicIndexShardInfo.getId());
        assertEquals(1234L, dynamicIndexShardInfo.getUpdateId());
        assertEquals(5678L, dynamicIndexShardInfo.getTimestamp());
        assertEquals(1, dynamicIndexShardInfo.getSubshardId());
        assertEquals(4, dynamicIndexShardInfo.getNumSubshards());

        // not dindex
        assertFalse(DynamicIndexSubshardDirnameUtil.tryParse("index19700101.00-19700101.01.1.4.1234.5678").isPresent());
    }

    @Test
    public void testSelectLatest() {
        final List<String> shardNames = ImmutableList.of(
                "dindex19700101.00-19700101.01.1.4.0.0"
                , "dindex19700101.00-19700101.01.1.4.1.4"
                , "dindex19700101.00-19700101.01.1.4.2.0"
                , "dindex19700101.00-19700101.01.1.4.2.1"
        );
        final Optional<String> latest = DynamicIndexSubshardDirnameUtil.selectLatest(
                "dindex19700101.00-19700101.01.1.4",
                shardNames,
                new Function<String, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable final String s) {
                        return s;
                    }
                }
        );
        assertTrue(latest.isPresent());
        assertEquals("dindex19700101.00-19700101.01.1.4.2.1", latest.get());
    }
}
