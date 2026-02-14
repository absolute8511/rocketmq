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
import java.util.List;
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
import org.apache.rocketmq.common.TopicConfig;
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
        // 让 topicConfigManager 暴露包含 NewTopic 的配置表，便于 syncConsumerOffset 收集到 topics
        ConcurrentHashMap<String, TopicConfig> topicTable = new ConcurrentHashMap<>();
        topicTable.put(newTopicConfig.getTopicName(), newTopicConfig);
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicTable);

        when(brokerOuterAPI.getAllTopicConfig(anyString())).thenReturn(createTopicConfigWrapper(newTopicConfig));
        // 新实现中，offset 同步走按 topic 增量接口
        when(brokerOuterAPI.getConsumerOffsetByTopicBatch(anyString(), any(List.class), anyLong())).thenReturn(createConsumerOffsetWrapper());
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

        when(brokerOuterAPI.getConsumerOffsetByTopicBatch(anyString(), any(List.class), anyLong())).thenReturn(wrapper);

        // 仅调用 syncConsumerOffset，避免依赖其它 syncXxx 行为
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // For new master, offsets should be merged incrementally into local table
        verify(consumerOffsetManager, times(1)).getOffsetTable();
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

        when(brokerOuterAPI.getConsumerOffsetByTopicBatch(anyString(), any(List.class), anyLong())).thenReturn(fullWrapper);

        // 仅调用 syncConsumerOffset，避免依赖其它 syncXxx 行为
        Method method = SlaveSynchronize.class.getDeclaredMethod("syncConsumerOffset");
        method.setAccessible(true);
        method.invoke(slaveSynchronize);

        // For old master, should be treated as full snapshot and overwrite local table via setOffsetTable
        verify(consumerOffsetManager, times(1)).setOffsetTable(any(ConcurrentHashMap.class));
        verify(consumerOffsetManager, times(1)).persist();
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

    private ConsumerOffsetSerializeWrapper createConsumerOffsetWrapper() {
        ConsumerOffsetSerializeWrapper wrapper = new ConsumerOffsetSerializeWrapper();
        // 构造一个包含 NewTopic@G1 的最小 offset 快照，便于触发增量同步路径
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
