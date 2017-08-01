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
        assertEquals(1234L, dynamicIndexShardInfo.getTimestamp());
        assertEquals(5678L, dynamicIndexShardInfo.getVersion());
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
