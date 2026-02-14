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
    public void testEncodeByTopicAndSince_filtersByTopicsAndTimestamp() {
        // Prepare offsetTable: topicA@G1, topicB@G1, topicC@G2
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsetsA = new ConcurrentHashMap<>();
        offsetsA.put(0, 10L);
        offsetTable.put("topicA" + TOPIC_GROUP_SEPARATOR + "G1", offsetsA);

        ConcurrentMap<Integer, Long> offsetsB = new ConcurrentHashMap<>();
        offsetsB.put(0, 20L);
        offsetTable.put("topicB" + TOPIC_GROUP_SEPARATOR + "G1", offsetsB);

        ConcurrentMap<Integer, Long> offsetsC = new ConcurrentHashMap<>();
        offsetsC.put(0, 30L);
        offsetTable.put("topicC" + TOPIC_GROUP_SEPARATOR + "G2", offsetsC);

        consumerOffsetManager.setOffsetTable(offsetTable);

        // Simulate timestamps: topicA earlier, topicB newer, topicC without timestamp
        long base = System.currentTimeMillis();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().clear();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().put("topicA", base - 10_000);
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().put("topicB", base);

        // Only request topicA/B, sinceTimestamp between their timestamps
        Set<String> topics = new HashSet<>();
        topics.add("topicA");
        topics.add("topicB");
        long since = base - 5_000;

        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeByTopicAndSince(topics, since);
        Map<String, ConcurrentMap<Integer, Long>> result = wrapper.getOffsetTable();

        // topicA last update is earlier than since, should be filtered
        assertThat(result).doesNotContainKey("topicA" + TOPIC_GROUP_SEPARATOR + "G1");
        // topicB matches topic and timestamp constraints, should be kept
        assertThat(result).containsKey("topicB" + TOPIC_GROUP_SEPARATOR + "G1");
        // topicC is not in whitelist, should be filtered
        assertThat(result).doesNotContainKey("topicC" + TOPIC_GROUP_SEPARATOR + "G2");
    }

    @Test
    public void testEncodeByTopicAndSince_filtersWhenTimestampEqualsSince() {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsetsA = new ConcurrentHashMap<>();
        offsetsA.put(0, 10L);
        offsetTable.put("topicA" + TOPIC_GROUP_SEPARATOR + "G1", offsetsA);

        consumerOffsetManager.setOffsetTable(offsetTable);

        long base = System.currentTimeMillis();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().clear();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().put("topicA", base);

        Set<String> topics = new HashSet<>();
        topics.add("topicA");

        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeByTopicAndSince(topics, base);
        Map<String, ConcurrentMap<Integer, Long>> result = wrapper.getOffsetTable();

        // lastUpdate == sinceTimestamp should also be filtered out
        assertThat(result).doesNotContainKey("topicA" + TOPIC_GROUP_SEPARATOR + "G1");
    }

    @Test
    public void testEncodeByTopicAndSince_noTopicsTreatsAsAllTopics() {
        // Build two records that are all recently updated
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsetsA = new ConcurrentHashMap<>();
        offsetsA.put(0, 10L);
        offsetTable.put("topicA" + TOPIC_GROUP_SEPARATOR + "G1", offsetsA);

        ConcurrentMap<Integer, Long> offsetsB = new ConcurrentHashMap<>();
        offsetsB.put(0, 20L);
        offsetTable.put("topicB" + TOPIC_GROUP_SEPARATOR + "G2", offsetsB);
        consumerOffsetManager.setOffsetTable(offsetTable);

        long now = System.currentTimeMillis();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().clear();
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().put("topicA", now);
        consumerOffsetManager.getTopicOffsetUpdateTimestampTable().put("topicB", now);

        // topics is null and sinceTimestamp<=0, treat as full snapshot
        ConsumerOffsetSerializeWrapper wrapper = consumerOffsetManager.encodeByTopicAndSince(null, 0);
        assertThat(wrapper.getOffsetTable()).hasSize(2);
    }
}
