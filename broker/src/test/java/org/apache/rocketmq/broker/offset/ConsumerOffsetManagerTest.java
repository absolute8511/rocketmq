/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.offset;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.rocketmq.broker.offset.ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private static final String KEY = "FooBar@FooBarGroup";

    private BrokerController brokerController;

    private ConsumerOffsetManager consumerOffsetManager;

    @Before
    @SuppressWarnings("DoubleBraceInitialization")
    public void init() {
        brokerController = Mockito.mock(BrokerController.class);
        consumerOffsetManager = new ConsumerOffsetManager(brokerController);

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        Mockito.when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>(512);
        offsetTable.put(KEY,new ConcurrentHashMap<Integer, Long>() {{
                put(1,2L);
                put(2,3L);
            }});
        consumerOffsetManager.setOffsetTable(offsetTable);
    }

    @Test
    public void cleanOffsetByTopic_NotExist() {
        consumerOffsetManager.cleanOffsetByTopic("InvalidTopic");
        assertThat(consumerOffsetManager.getOffsetTable().containsKey(KEY)).isTrue();
    }

    @Test
    public void cleanOffsetByTopic_Exist() {
        consumerOffsetManager.cleanOffsetByTopic("FooBar");
        assertThat(!consumerOffsetManager.getOffsetTable().containsKey(KEY)).isTrue();
    }

    @Test
    public void removeOffsetByGroupTest() {
        String topic = "TopicName";
        String group = "GroupName";
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        consumerOffsetManager.commitOffset("Commit", group, topic, 0, 100);
        consumerOffsetManager.assignResetOffset(topic, group, 0, 100);
        consumerOffsetManager.commitPullOffset("Pull", group, topic, 0, 100);
        consumerOffsetManager.removeOffset(group);
        Assert.assertFalse(consumerOffsetManager.getOffsetTable().containsKey(topic + TOPIC_GROUP_SEPARATOR + group));

        consumerOffsetManager.commitPullOffset("Pull", group, topic, 0, 100);
        consumerOffsetManager.clearPullOffset(group, topic);
        Assert.assertEquals(-1L, consumerOffsetManager.queryPullOffset(group, topic, 0));
    }

    @Test
    public void testOffsetPersistInMemory() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = consumerOffsetManager.getOffsetTable();
        ConcurrentMap<Integer, Long> table = new ConcurrentHashMap<>();
        table.put(0, 1L);
        table.put(1, 3L);
        String group = "G1";
        offsetTable.put(group, table);

        consumerOffsetManager.persist();
        ConsumerOffsetManager manager = new ConsumerOffsetManager(brokerController);
        manager.load();

        ConcurrentMap<Integer, Long> offsetTableLoaded = manager.getOffsetTable().get(group);
        Assert.assertEquals(table, offsetTableLoaded);
    }

    @Test
    public void testCleanOffsetByTopic_cleansAllOffsetTables() {
        String topic = "TopicName";
        String group = "GroupName";
        Mockito.when(brokerController.getBrokerConfig()).thenReturn(new BrokerConfig());
        consumerOffsetManager.commitOffset("Commit", group, topic, 0, 100);
        consumerOffsetManager.assignResetOffset(topic, group, 1, 200);
        consumerOffsetManager.commitPullOffset("Pull", group, topic, 2, 300);

        Assert.assertEquals(100L, consumerOffsetManager.queryOffset(group, topic, 0));
        Assert.assertTrue(consumerOffsetManager.hasOffsetReset(topic, group, 1));
        Assert.assertEquals(300L, consumerOffsetManager.queryPullOffset(group, topic, 2));

        consumerOffsetManager.cleanOffsetByTopic(topic);

        Assert.assertEquals(-1L, consumerOffsetManager.queryOffset(group, topic, 0));
        Assert.assertFalse(consumerOffsetManager.hasOffsetReset(topic, group, 1));
        Assert.assertEquals(-1L, consumerOffsetManager.queryPullOffset(group, topic, 2));
    }

    @Test
    public void testEncodeNormalOffset_excludesLmqOffsets() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> normalOffsets = new ConcurrentHashMap<>();
        normalOffsets.put(0, 10L);
        offsetTable.put("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1", normalOffsets);

        ConcurrentMap<Integer, Long> lmqOffsets = new ConcurrentHashMap<>();
        lmqOffsets.put(0, 20L);
        String lmqTopic = MixAll.LMQ_PREFIX + "liteTopic";
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G1", lmqOffsets);
        consumerOffsetManager.setOffsetTable(offsetTable);

        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeNormalOffset();

        assertThat(wrapper.getOffsetTable()).containsOnlyKeys("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1");
        Assert.assertEquals(10L, wrapper.getOffsetTable().get("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1").get(0).longValue());
    }

    @Test
    public void testEncodeLmqByGroupAndSince_filtersByGroupAndTimestamp() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        String lmqTopic = MixAll.LMQ_PREFIX + "liteTopic";
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G1", offsetMap(0, 10L));
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G2", offsetMap(0, 20L));
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G3", offsetMap(0, 30L));
        offsetTable.put("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1", offsetMap(0, 40L));
        consumerOffsetManager.setOffsetTable(offsetTable);

        long now = System.currentTimeMillis();
        consumerOffsetManager.getGroupOffsetUpdateTimestampTable().clear();
        consumerOffsetManager.getGroupOffsetUpdateTimestampTable().put("G1", now);
        consumerOffsetManager.getGroupOffsetUpdateTimestampTable().put("G2", now - 10_000);

        Set<String> groups = new HashSet<>();
        groups.add("G1");
        groups.add("G2");
        groups.add("G3");
        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeLmqByGroupAndSince(groups, now - 5_000);
        Map<String, ConcurrentMap<Integer, Long>> result = wrapper.getOffsetTable();

        assertThat(result).containsKey(lmqTopic + TOPIC_GROUP_SEPARATOR + "G1");
        assertThat(result).doesNotContainKey(lmqTopic + TOPIC_GROUP_SEPARATOR + "G2");
        assertThat(result).containsKey(lmqTopic + TOPIC_GROUP_SEPARATOR + "G3");
        assertThat(result).doesNotContainKey("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1");
    }

    @Test
    public void testEncodeLmqByGroupAndSince_nullGroupsTreatsAsAllGroups() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        String lmqTopic = MixAll.LMQ_PREFIX + "liteTopic";
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G1", offsetMap(0, 10L));
        offsetTable.put(lmqTopic + TOPIC_GROUP_SEPARATOR + "G2", offsetMap(0, 20L));
        offsetTable.put("normalTopic" + TOPIC_GROUP_SEPARATOR + "G1", offsetMap(0, 30L));
        consumerOffsetManager.setOffsetTable(offsetTable);

        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeLmqByGroupAndSince(null, 0);

        assertThat(wrapper.getOffsetTable()).containsOnlyKeys(
            lmqTopic + TOPIC_GROUP_SEPARATOR + "G1",
            lmqTopic + TOPIC_GROUP_SEPARATOR + "G2");
    }

    private ConcurrentMap<Integer, Long> offsetMap(int queueId, long offset) {
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(queueId, offset);
        return offsets;
    }
}
