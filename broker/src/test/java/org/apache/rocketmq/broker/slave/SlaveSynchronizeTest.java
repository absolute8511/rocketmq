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
package org.apache.rocketmq.broker.slave;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.processor.QueryAssignmentProcessor;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SlaveSynchronizeTest {
    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(), new MessageStoreConfig());

    private SlaveSynchronize slaveSynchronize;

    @Mock
    private BrokerOuterAPI brokerOuterAPI;

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Mock
    private MessageStore messageStore;

    @Mock
    private ScheduleMessageService scheduleMessageService;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private QueryAssignmentProcessor queryAssignmentProcessor;

    @Mock
    private MessageRequestModeManager messageRequestModeManager;

    @Mock
    private TimerMessageStore timerMessageStore;

    @Mock
    private TimerMetrics timerMetrics;

    @Mock
    private TimerCheckpoint timerCheckpoint;

    private static final String BROKER_ADDR = "127.0.0.1:10911";

    @Before
    public void init() {
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getQueryAssignmentProcessor()).thenReturn(queryAssignmentProcessor);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(brokerController.getTimerCheckpoint()).thenReturn(timerCheckpoint);
        when(topicConfigManager.getDataVersion()).thenReturn(new DataVersion());
        when(topicConfigManager.getTopicConfigTable()).thenReturn(new ConcurrentHashMap<>());
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());
        when(consumerOffsetManager.getDataVersion()).thenReturn(new DataVersion());
        when(subscriptionGroupManager.getDataVersion()).thenReturn(new DataVersion());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(new ConcurrentHashMap<>());
        when(queryAssignmentProcessor.getMessageRequestModeManager()).thenReturn(messageRequestModeManager);
        when(messageRequestModeManager.getMessageRequestModeMap()).thenReturn(new ConcurrentHashMap<>());
        when(messageStoreConfig.isTimerWheelEnable()).thenReturn(true);
        when(messageStore.getTimerMessageStore()).thenReturn(timerMessageStore);
        when(timerMessageStore.isShouldRunningDequeue()).thenReturn(false);
        when(timerMessageStore.getTimerMetrics()).thenReturn(timerMetrics);
        when(timerMetrics.getDataVersion()).thenReturn(new DataVersion());
        when(timerCheckpoint.getDataVersion()).thenReturn(new DataVersion());
        slaveSynchronize = new SlaveSynchronize(brokerController);
        slaveSynchronize.setMasterAddr(BROKER_ADDR);
    }

    @Test
    public void testSyncAll() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        MQBrokerException, InterruptedException, UnsupportedEncodingException, RemotingCommandException {
        TopicConfig newTopicConfig = new TopicConfig("NewTopic");
        // Make topicConfigManager expose topic table containing NewTopic so syncConsumerOffset can collect topics
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put(newTopicConfig.getTopicName(), newTopicConfig);
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        when(brokerOuterAPI.getAllTopicConfig(anyString())).thenReturn(createTopicConfigWrapper(newTopicConfig));
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(createConsumerOffsetWrapper());
        when(consumerOffsetManager.getDataVersion()).thenReturn(createChangedDataVersion());
        when(brokerOuterAPI.getAllDelayOffset(anyString())).thenReturn("");
        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(createSubscriptionGroupWrapper());
        when(brokerOuterAPI.getAllMessageRequestMode(anyString())).thenReturn(createMessageRequestModeWrapper());
        when(brokerOuterAPI.getTimerMetrics(anyString())).thenReturn(createTimerMetricsWrapper());
        slaveSynchronize.syncAll();
        Assert.assertEquals(1, this.brokerController.getTopicConfigManager().getDataVersion().getStateVersion());
        Assert.assertEquals(1, this.brokerController.getTopicQueueMappingManager().getDataVersion().getStateVersion());
        Assert.assertEquals(1, consumerOffsetManager.getDataVersion().getStateVersion());
        Assert.assertEquals(1, subscriptionGroupManager.getDataVersion().getStateVersion());
        Assert.assertEquals(1, timerMetrics.getDataVersion().getStateVersion());
    }

    @Test
    public void testSyncConsumerOffsetIncrementalWithNewMaster() throws Exception {
        // Build topic config table on broker side
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        topicTable.put("topicB", new TopicConfig("topicB"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // New master returns incremental result filtered by topics, only topicA
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(0, 100L);
        offsetTable.put("topicA@G1", offsets);
        wrapper.setOffsetTable(offsetTable);
        wrapper.setDataVersion(new DataVersion());

        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(wrapper);

        // Invoke only syncConsumerOffset to avoid dependency on other syncXxx behaviors
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // Normal offsets should be merged through commitOffset instead of replacing the whole table.
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", "topicA", 0, 100L);
        verify(consumerOffsetManager, times(0)).setOffsetTable(any(ConcurrentHashMap.class));
        verify(consumerOffsetManager, times(1)).persist();
    }

    @Test
    public void testSyncConsumerOffsetFallbackToFullSnapshotWithOldMaster() throws Exception {
        // Build topic config table on broker side, only topicA
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // Old master returns a full snapshot containing topicA and topicX
        ConsumerOffsetSerializeWrapper fullWrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> fullOffsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsetsA = new ConcurrentHashMap<>();
        offsetsA.put(0, 100L);
        fullOffsetTable.put("topicA@G1", offsetsA);
        ConcurrentMap<Integer, Long> offsetsX = new ConcurrentHashMap<>();
        offsetsX.put(0, 200L);
        fullOffsetTable.put("topicX@G2", offsetsX);
        fullWrapper.setOffsetTable(fullOffsetTable);
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(2L);
        fullWrapper.setDataVersion(dataVersion);

        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(fullWrapper);

        // Invoke only syncConsumerOffset to avoid dependency on other syncXxx behaviors
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // Normal offsets are synced by merging each master normal snapshot entry.
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", "topicA", 0, 100L);
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G2", "topicX", 0, 200L);
        verify(consumerOffsetManager, times(0)).setOffsetTable(any(ConcurrentHashMap.class));
        verify(consumerOffsetManager, times(1)).persist();
    }

    @Test
    public void testSyncConsumerOffsetUsesLastSyncTimestamp() throws Exception {
        // Build non-empty topicConfigTable to avoid early return
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // Disable safe gap so that sinceTimestamp equals lastConsumerOffsetSyncTimestamp
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        brokerConfig.setSyncConsumerOffsetSafeGapMillis(0);

        // Set lastConsumerOffsetSyncTimestamp to a specific value via reflection
        long lastSyncTs = 123456L;
        java.lang.reflect.Field field = SlaveSynchronize.class.getDeclaredField("lastConsumerOffsetSyncTimestamp");
        field.setAccessible(true);
        field.setLong(slaveSynchronize, lastSyncTs);

        // Stub offsetWrapper returned by master
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(0, 100L);
        offsetTable.put("topicA@G1", offsets);
        wrapper.setOffsetTable(offsetTable);
        wrapper.setDataVersion(new DataVersion());

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());

        org.mockito.ArgumentCaptor<Long> sinceCaptor = org.mockito.ArgumentCaptor.forClass(Long.class);
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups = new ConcurrentHashMap<>();
        subscriptionGroups.put("G1", new SubscriptionGroupConfig());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroups);
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());
        when(brokerOuterAPI.getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), sinceCaptor.capture()))
            .thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // Verify sinceTimestamp passed to master equals lastConsumerOffsetSyncTimestamp
        Assert.assertEquals(lastSyncTs, sinceCaptor.getValue().longValue());
    }

    @Test
    public void testSyncConsumerOffsetAppliesSafeGap() throws Exception {
        // Build non-empty topicConfigTable to avoid early return
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // Configure a non-zero safe gap
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        long safeGap = 5000L;
        brokerConfig.setSyncConsumerOffsetSafeGapMillis(safeGap);

        // Set lastConsumerOffsetSyncTimestamp to a specific value via reflection
        long lastSyncTs = 20_000L;
        java.lang.reflect.Field field = SlaveSynchronize.class.getDeclaredField("lastConsumerOffsetSyncTimestamp");
        field.setAccessible(true);
        field.setLong(slaveSynchronize, lastSyncTs);

        // Stub offsetWrapper returned by master
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(0, 100L);
        offsetTable.put("topicA@G1", offsets);
        wrapper.setOffsetTable(offsetTable);
        wrapper.setDataVersion(new DataVersion());

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());

        org.mockito.ArgumentCaptor<Long> sinceCaptor = org.mockito.ArgumentCaptor.forClass(Long.class);
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups = new ConcurrentHashMap<>();
        subscriptionGroups.put("G1", new SubscriptionGroupConfig());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroups);
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());
        when(brokerOuterAPI.getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), sinceCaptor.capture()))
            .thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        long expectedSince = lastSyncTs - safeGap;
        Assert.assertEquals(expectedSince, sinceCaptor.getValue().longValue());
    }

    @Test
    public void testSyncConsumerOffsetBatchesTopicsByConfiguredSize() throws Exception {
        // Build five groups
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups = new ConcurrentHashMap<>();
        for (int i = 0; i < 5; i++) {
            subscriptionGroups.put("G" + i, new SubscriptionGroupConfig());
        }
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroups);

        // Set batch size = 2
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        brokerConfig.setSyncConsumerOffsetBatchNum(2);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());

        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());
        // Stub master to always return offsets that belong to the current group batch
        org.mockito.stubbing.Answer<ConsumerOffsetSerializeWrapper> answer = invocation -> {
            @SuppressWarnings("unchecked")
            List<String> batchGroups = (List<String>) invocation.getArgument(1);
            ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
            ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
            ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
            offsets.put(0, 100L);
            if (!batchGroups.isEmpty()) {
                offsetTable.put("%LMQ%T@" + batchGroups.get(0), offsets);
            }
            wrapper.setOffsetTable(offsetTable);
            wrapper.setDataVersion(new DataVersion());
            return wrapper;
        };

        when(brokerOuterAPI.getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), anyLong()))
            .thenAnswer(answer);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // For 5 groups and batch size 2, should be invoked 3 times
        org.mockito.Mockito.verify(brokerOuterAPI, org.mockito.Mockito.times(3))
            .getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), anyLong());
    }

    @Test
    public void testSyncConsumerOffsetCleansOffsetsForDeletedTopicsOnly() throws Exception {
        // topicConfigTable only contains existingTopic, deletedTopic has been removed on master
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("existingTopic", new TopicConfig("existingTopic"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // Local offset table still keeps both existingTopic and deletedTopic offsets
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Long> existOffsets = new ConcurrentHashMap<>();
        existOffsets.put(0, 100L);
        localOffsetTable.put("existingTopic@G1", existOffsets);

        ConcurrentHashMap<Integer, Long> deletedOffsets = new ConcurrentHashMap<>();
        deletedOffsets.put(0, 200L);
        localOffsetTable.put("deletedTopic@G1", deletedOffsets);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);
        stubCommitOffset(localOffsetTable);
        org.mockito.Mockito.doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            localOffsetTable.keySet().removeIf(key -> key.startsWith(topic + ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR));
            return null;
        }).when(consumerOffsetManager).cleanOffsetByTopic(anyString());

        // Master returns incremental offsets only for existingTopic
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> remoteOffsets = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> remoteExistOffsets = new ConcurrentHashMap<>();
        remoteExistOffsets.put(0, 150L);
        remoteOffsets.put("existingTopic@G1", remoteExistOffsets);
        wrapper.setOffsetTable(remoteOffsets);
        wrapper.setDataVersion(new DataVersion());

        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // Offsets for deletedTopic should be removed, existingTopic should be kept and updated
        Assert.assertFalse(localOffsetTable.containsKey("deletedTopic@G1"));
        Assert.assertTrue(localOffsetTable.containsKey("existingTopic@G1"));
        Assert.assertEquals(150L, localOffsetTable.get("existingTopic@G1").get(0).longValue());
        // Also verify cleanOffsetByTopic is invoked for deletedTopic, so all related offset tables can be cleaned.
        verify(consumerOffsetManager, times(1)).cleanOffsetByTopic("deletedTopic");
    }

    @Test
    public void testSyncConsumerOffsetCleansStaleNormalOffsetWhenMasterSnapshotDoesNotContainTopic() throws Exception {
        // topicConfigTable contains topicA so it is treated as existing
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        // Local offset table has topicA offsets before sync
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Long> offsetsA = new ConcurrentHashMap<>();
        offsetsA.put(0, 100L);
        localOffsetTable.put("topicA@G1", offsetsA);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);

        // Master returns a full normal-only snapshot without topicA
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        wrapper.setOffsetTable(new ConcurrentHashMap<>());
        wrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // Normal offsets absent from the master normal snapshot are explicitly cleaned.
        Assert.assertFalse(localOffsetTable.containsKey("topicA@G1"));
        // Topic cleanup is not responsible for topicA because the topic still exists.
        verify(consumerOffsetManager, times(1)).removeConsumerOffset("topicA@G1");
        verify(consumerOffsetManager, times(0)).setOffsetTable(any(ConcurrentHashMap.class));
    }

    @Test
    public void testSyncConsumerOffsetCleansStaleNormalQueueWhenMasterSnapshotDoesNotContainQueue() throws Exception {
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("topicA", new TopicConfig("topicA"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> localQueues = new ConcurrentHashMap<>();
        localQueues.put(0, 100L);
        localQueues.put(1, 200L);
        localQueues.put(2, 300L);
        localOffsetTable.put("topicA@G1", localQueues);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);
        stubCommitOffset(localOffsetTable);

        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> remoteOffsets = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> remoteQueues = new ConcurrentHashMap<>();
        remoteQueues.put(0, 150L);
        remoteQueues.put(2, 350L);
        remoteOffsets.put("topicA@G1", remoteQueues);
        wrapper.setOffsetTable(remoteOffsets);
        wrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        Assert.assertTrue(localOffsetTable.containsKey("topicA@G1"));
        Assert.assertEquals(2, localOffsetTable.get("topicA@G1").size());
        Assert.assertEquals(150L, localOffsetTable.get("topicA@G1").get(0).longValue());
        Assert.assertFalse(localOffsetTable.get("topicA@G1").containsKey(1));
        Assert.assertEquals(350L, localOffsetTable.get("topicA@G1").get(2).longValue());
        verify(consumerOffsetManager, times(1)).removeConsumerOffset("topicA@G1");
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", "topicA", 0, 150L);
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", "topicA", 2, 350L);
        verify(consumerOffsetManager, times(0)).setOffsetTable(any(ConcurrentHashMap.class));
    }

    @Test
    public void testSyncConsumerOffsetMergesNormalOffsetsAndPreservesLocalLmqOffsets() throws Exception {
        String lmqParentTopic = "lmqParentTopic";
        String lmqTopic = LiteUtil.toLmqName(lmqParentTopic, "LiteTopic");
        String staleRemoteLmqTopic = LiteUtil.toLmqName(lmqParentTopic, "StaleRemoteLiteTopic");
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("remoteNormalTopic", new TopicConfig("remoteNormalTopic"));
        topicTable.put("staleNormalTopic", new TopicConfig("staleNormalTopic"));
        topicTable.put(lmqParentTopic, new TopicConfig(lmqParentTopic));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        localOffsetTable.put("staleNormalTopic@G1", offsetMap(0, 10L));
        localOffsetTable.put(lmqTopic + "@G1", offsetMap(0, 20L));
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);
        stubCommitOffset(localOffsetTable);

        ConsumerOffsetSerializeWrapper normalWrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> remoteNormalOffsets = new ConcurrentHashMap<>();
        remoteNormalOffsets.put("remoteNormalTopic@G1", offsetMap(0, 100L));
        remoteNormalOffsets.put(staleRemoteLmqTopic + "@G1", offsetMap(0, 200L));
        normalWrapper.setOffsetTable(remoteNormalOffsets);
        normalWrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(normalWrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        Assert.assertFalse(localOffsetTable.containsKey("staleNormalTopic@G1"));
        Assert.assertEquals(100L, localOffsetTable.get("remoteNormalTopic@G1").get(0).longValue());
        Assert.assertEquals(20L, localOffsetTable.get(lmqTopic + "@G1").get(0).longValue());
        Assert.assertFalse(localOffsetTable.containsKey(staleRemoteLmqTopic + "@G1"));
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", "remoteNormalTopic", 0, 100L);
        verify(consumerOffsetManager, times(0)).commitOffset(null, "G1", staleRemoteLmqTopic, 0, 200L);
        verify(consumerOffsetManager, times(0)).setOffsetTable(any(ConcurrentHashMap.class));
    }

    @Test
    public void testSyncConsumerOffsetMergesLmqOffsetsThroughCommitOffset() throws Exception {
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("normalTopic", new TopicConfig("normalTopic"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups = new ConcurrentHashMap<>();
        subscriptionGroups.put("G1", new SubscriptionGroupConfig());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroups);
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());

        String lmqTopic = MixAll.LMQ_PREFIX + "LiteTopic";
        ConsumerOffsetSerializeWrapper lmqWrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> lmqOffsets = new ConcurrentHashMap<>();
        lmqOffsets.put(lmqTopic + "@G1", offsetMap(0, 333L));
        lmqWrapper.setOffsetTable(lmqOffsets);
        lmqWrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), anyLong()))
            .thenReturn(lmqWrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", lmqTopic, 0, 333L);
    }

    @Test
    public void testSyncConsumerOffsetDoesNotCleanMissingLmqOffsetsInIncrementalResult() throws Exception {
        String parentTopic = "parentTopic";
        String lmqTopic = LiteUtil.toLmqName(parentTopic, "childTopic");
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put(parentTopic, new TopicConfig(parentTopic));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroups = new ConcurrentHashMap<>();
        subscriptionGroups.put("G1", new SubscriptionGroupConfig());
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroups);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> localQueues = new ConcurrentHashMap<>();
        localQueues.put(0, 100L);
        localQueues.put(1, 200L);
        localOffsetTable.put(lmqTopic + "@G1", localQueues);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);
        stubCommitOffset(localOffsetTable);

        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());

        ConsumerOffsetSerializeWrapper lmqWrapper = new ConsumerOffsetSerializeWrapper();
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> lmqOffsets = new ConcurrentHashMap<>();
        lmqOffsets.put(lmqTopic + "@G1", offsetMap(0, 150L));
        lmqWrapper.setOffsetTable(lmqOffsets);
        lmqWrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getLmqConsumerOffsetByGroupBatch(anyString(), any(List.class), anyLong()))
            .thenReturn(lmqWrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        Assert.assertTrue(localOffsetTable.containsKey(lmqTopic + "@G1"));
        Assert.assertEquals(2, localOffsetTable.get(lmqTopic + "@G1").size());
        Assert.assertEquals(150L, localOffsetTable.get(lmqTopic + "@G1").get(0).longValue());
        Assert.assertEquals(200L, localOffsetTable.get(lmqTopic + "@G1").get(1).longValue());
        verify(consumerOffsetManager, times(1)).commitOffset(null, "G1", lmqTopic, 0, 150L);
        verify(consumerOffsetManager, times(0)).removeConsumerOffset(lmqTopic + "@G1");
        verify(consumerOffsetManager, times(0)).removeOffset("G1");
    }

    @Test
    public void testSyncConsumerOffsetCleansLmqOffsetsByParentTopicExistence() throws Exception {
        String parentTopic = "parentTopic";
        String existingLmqTopic = LiteUtil.toLmqName(parentTopic, "childTopic");
        String deletedLmqTopic = LiteUtil.toLmqName("deletedParentTopic", "childTopic");
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put(parentTopic, new TopicConfig(parentTopic));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        localOffsetTable.put(existingLmqTopic + "@G1", offsetMap(0, 100L));
        localOffsetTable.put(deletedLmqTopic + "@G1", offsetMap(0, 200L));
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);
        org.mockito.Mockito.doAnswer(invocation -> {
            String topic = invocation.getArgument(0);
            localOffsetTable.keySet().removeIf(key -> key.startsWith(topic + ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR));
            return null;
        }).when(consumerOffsetManager).cleanOffsetByTopic(anyString());
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(new ConsumerOffsetSerializeWrapper());

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        Assert.assertTrue(localOffsetTable.containsKey(existingLmqTopic + "@G1"));
        Assert.assertFalse(localOffsetTable.containsKey(deletedLmqTopic + "@G1"));
        verify(consumerOffsetManager, times(0)).cleanOffsetByTopic(existingLmqTopic);
        verify(consumerOffsetManager, times(1)).cleanOffsetByTopic(deletedLmqTopic);
    }

    @Test
    public void testSyncConsumerOffsetCleansOffsetsWhenTopicTableEmpty() throws Exception {
        // topicConfigTable is empty after master deletes all topics
        when(topicConfigManager.getTopicConfigTable()).thenReturn(new ConcurrentHashMap<>());

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Long> deletedOffsets = new ConcurrentHashMap<>();
        deletedOffsets.put(0, 200L);
        localOffsetTable.put("deletedTopic@G1", deletedOffsets);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        verify(brokerOuterAPI, times(1)).getNormalConsumerOffset(anyString());
        verify(consumerOffsetManager, times(1)).cleanOffsetByTopic("deletedTopic");
        verify(consumerOffsetManager, times(1)).persist();
    }

    @Test
    public void testSyncConsumerOffsetCleansDeletedTopicByManager() throws Exception {
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put("existingTopic", new TopicConfig("existingTopic"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Long> deletedOffsets = new ConcurrentHashMap<>();
        deletedOffsets.put(0, 200L);
        localOffsetTable.put("deletedTopic@G1", deletedOffsets);

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);

        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        wrapper.setOffsetTable(new ConcurrentHashMap<>());
        wrapper.setDataVersion(new DataVersion());
        when(brokerOuterAPI.getNormalConsumerOffset(anyString())).thenReturn(wrapper);

        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        verify(consumerOffsetManager, times(1)).cleanOffsetByTopic("deletedTopic");
        verify(consumerOffsetManager, times(1)).removeConsumerOffset("deletedTopic@G1");
    }

    @Test
    public void testSyncSubscriptionGroupConfigCleansOffsetsWhenEnabled() throws Exception {
        // Enable group-offset cleanup in slave
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        brokerConfig.setCleanDeletedSubscriptionGroupOffsetInSlave(true);

        // Current subscription groups on slave: G1, G2
        ConcurrentHashMap<String, SubscriptionGroupConfig> curTable = new ConcurrentHashMap<>();
        curTable.put("G1", new SubscriptionGroupConfig());
        curTable.put("G2", new SubscriptionGroupConfig());

        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(curTable);
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        localOffsetTable.put("topicA@G1", offsetMap(0, 100L));
        localOffsetTable.put("topicB@G2", offsetMap(0, 200L));
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);

        // Master only keeps G1 now
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> newTable = new ConcurrentHashMap<>();
        newTable.put("G1", new SubscriptionGroupConfig());
        wrapper.setSubscriptionGroupTable(newTable);
        wrapper.setDataVersion(createChangedDataVersion());

        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(wrapper);

        // Invoke syncSubscriptionGroupConfig via reflection
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncSubscriptionGroupConfig");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // G2 should be removed from local table
        Assert.assertFalse(curTable.containsKey("G2"));
        // And removeOffset should be called for group G2
        verify(consumerOffsetManager, times(1)).removeOffset("G2");
    }

    @Test
    public void testSyncSubscriptionGroupConfigKeepsOffsetsWhenDisabled() throws Exception {
        // Disable group-offset cleanup in slave (default false, set explicitly for clarity)
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        brokerConfig.setCleanDeletedSubscriptionGroupOffsetInSlave(false);

        // Current subscription groups on slave: G1, G2
        ConcurrentHashMap<String, SubscriptionGroupConfig> curTable = new ConcurrentHashMap<>();
        curTable.put("G1", new SubscriptionGroupConfig());
        curTable.put("G2", new SubscriptionGroupConfig());

        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(curTable);

        // Master only keeps G1 now
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> newTable = new ConcurrentHashMap<>();
        newTable.put("G1", new SubscriptionGroupConfig());
        wrapper.setSubscriptionGroupTable(newTable);
        wrapper.setDataVersion(createChangedDataVersion());

        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(wrapper);

        // Invoke syncSubscriptionGroupConfig via reflection
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncSubscriptionGroupConfig");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // G2 should be removed from local subscriptionGroupTable (config), as before
        Assert.assertFalse(curTable.containsKey("G2"));
        // But removeOffset should NOT be called when the switch is disabled
        verify(consumerOffsetManager, times(0)).removeOffset("G2");
    }

    @Test
    public void testSyncSubscriptionGroupConfigSkipsOffsetCleanupWhenGroupHasRecentOffsets() throws Exception {
        // Enable group-offset cleanup in slave
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        brokerConfig.setCleanDeletedSubscriptionGroupOffsetInSlave(true);

        // Current subscription groups on slave: G1, G2
        ConcurrentHashMap<String, SubscriptionGroupConfig> curTable = new ConcurrentHashMap<>();
        curTable.put("G1", new SubscriptionGroupConfig());
        curTable.put("G2", new SubscriptionGroupConfig());

        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(curTable);
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> localOffsetTable = new ConcurrentHashMap<>();
        localOffsetTable.put("topicB@G2", offsetMap(0, 200L));
        when(consumerOffsetManager.getOffsetTable()).thenReturn(localOffsetTable);

        // Master only keeps G1 now
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> newTable = new ConcurrentHashMap<>();
        newTable.put("G1", new SubscriptionGroupConfig());
        wrapper.setSubscriptionGroupTable(newTable);
        wrapper.setDataVersion(createChangedDataVersion());

        when(brokerOuterAPI.getAllSubscriptionGroupConfig(anyString())).thenReturn(wrapper);

        // Simulate that G2 still had offsets in the latest consumer offset sync round
        java.lang.reflect.Field field = SlaveSynchronize.class.getDeclaredField("lastSyncedConsumerOffsetGroups");
        field.setAccessible(true);
        Set<String> groups = new HashSet<>();
        groups.add("G2");
        field.set(slaveSynchronize, groups);

        // Invoke syncSubscriptionGroupConfig via reflection
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncSubscriptionGroupConfig");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // G2 should be removed from local subscriptionGroupTable (config), as before
        Assert.assertFalse(curTable.containsKey("G2"));
        // But removeOffset should NOT be called for G2 because it still appeared in last offset sync
        verify(consumerOffsetManager, times(0)).removeOffset("G2");
    }

    @Test
    public void testGetMasterAddr() {
        Assert.assertEquals(BROKER_ADDR, slaveSynchronize.getMasterAddr());
    }

    @Test
    public void testSyncTimerCheckPoint() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(brokerOuterAPI.getTimerCheckPoint(anyString())).thenReturn(timerCheckpoint);
        slaveSynchronize.syncTimerCheckPoint();
        Assert.assertEquals(0, timerCheckpoint.getDataVersion().getStateVersion());
    }

    private TopicConfigAndMappingSerializeWrapper createTopicConfigWrapper(TopicConfig topicConfig) {
        TopicConfigAndMappingSerializeWrapper wrapper = new TopicConfigAndMappingSerializeWrapper();
        wrapper.setTopicConfigTable(new ConcurrentHashMap<>());
        wrapper.getTopicConfigTable().put(topicConfig.getTopicName(), topicConfig);
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        wrapper.setMappingDataVersion(dataVersion);
        return wrapper;
    }

    private DataVersion createChangedDataVersion() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        return dataVersion;
    }

    private ConsumerOffsetSerializeWrapper createConsumerOffsetWrapper() {
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        // Build a minimal offset snapshot containing NewTopic@G1 to trigger incremental sync path
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(0, 100L);
        offsetTable.put("NewTopic@G1", offsets);
        wrapper.setOffsetTable(offsetTable);
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }

    private void stubCommitOffset(ConcurrentMap<String, ConcurrentMap<Integer, Long>> localOffsetTable) {
        org.mockito.Mockito.doAnswer(invocation -> {
            String group = invocation.getArgument(1);
            String topic = invocation.getArgument(2);
            int queueId = invocation.getArgument(3);
            long offset = invocation.getArgument(4);
            localOffsetTable.computeIfAbsent(topic + ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR + group,
                key -> new ConcurrentHashMap<>()).put(queueId, offset);
            return null;
        }).when(consumerOffsetManager).commitOffset(any(), anyString(), anyString(), anyInt(), anyLong());
    }

    private ConcurrentMap<Integer, Long> offsetMap(int queueId, long offset) {
        ConcurrentMap<Integer, Long> offsets = new ConcurrentHashMap<>();
        offsets.put(queueId, offset);
        return offsets;
    }

    private SubscriptionGroupWrapper createSubscriptionGroupWrapper() {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        wrapper.setSubscriptionGroupTable(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }

    private MessageRequestModeSerializeWrapper createMessageRequestModeWrapper() {
        MessageRequestModeSerializeWrapper wrapper = new MessageRequestModeSerializeWrapper();
        wrapper.setMessageRequestModeMap(new ConcurrentHashMap<>());
        return wrapper;
    }

    private TimerMetrics.TimerMetricsSerializeWrapper createTimerMetricsWrapper() {
        TimerMetrics.TimerMetricsSerializeWrapper wrapper = new TimerMetrics.TimerMetricsSerializeWrapper();
        wrapper.setTimingCount(new ConcurrentHashMap<>());
        DataVersion dataVersion = new DataVersion();
        dataVersion.setStateVersion(1L);
        wrapper.setDataVersion(dataVersion);
        return wrapper;
    }
}
