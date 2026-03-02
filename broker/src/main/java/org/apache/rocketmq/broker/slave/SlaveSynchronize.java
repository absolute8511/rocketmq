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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.loadbalance.MessageRequestModeManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.MessageRequestModeSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMetrics;

public class SlaveSynchronize {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private volatile String masterAddr = null;

    /**
     * Last successful consumer offset sync finish time on this slave.
     * Used as lower bound when requesting incremental offsets from
     * master.
     */
    private volatile long lastConsumerOffsetSyncTimestamp = 0L;

    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        if (!StringUtils.equals(this.masterAddr, masterAddr)) {
            LOGGER.info("Update master address from {} to {}", this.masterAddr, masterAddr);
            this.masterAddr = masterAddr;
        }
    }

    public void syncAll() {
        this.syncTopicConfig();
        this.syncConsumerOffset();
        this.syncDelayOffset();
        this.syncSubscriptionGroupConfig();
        this.syncMessageRequestMode();

        if (brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            this.syncTimerMetrics();
        }
    }

    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                TopicConfigAndMappingSerializeWrapper topicWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                TopicConfigManager topicConfigManager = this.brokerController.getTopicConfigManager();
                if (!topicConfigManager.getDataVersion().equals(topicWrapper.getDataVersion())) {

                    topicConfigManager.getDataVersion().assignNewOne(topicWrapper.getDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigManager.getTopicConfigTable();

                    //delete
                    Iterator<Map.Entry<String, TopicConfig>> iterator = topicConfigTable.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, TopicConfig> entry = iterator.next();
                        if (!newTopicConfigTable.containsKey(entry.getKey())) {
                            iterator.remove();
                        }
                        topicConfigManager.deleteTopicConfig(entry.getKey());
                    }

                    //update
                    newTopicConfigTable.values().forEach(topicConfigManager::putTopicConfig);
                    topicConfigManager.updateDataVersion();
                    topicConfigManager.persist();
                }
                if (topicWrapper.getTopicQueueMappingDetailMap() != null
                        && !topicWrapper.getMappingDataVersion().equals(this.brokerController.getTopicQueueMappingManager().getDataVersion())) {
                    this.brokerController.getTopicQueueMappingManager().getDataVersion()
                            .assignNewOne(topicWrapper.getMappingDataVersion());

                    ConcurrentMap<String, TopicConfig> newTopicConfigTable = topicWrapper.getTopicConfigTable();
                    //delete
                    ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
                    topicConfigTable.entrySet().removeIf(item -> !newTopicConfigTable.containsKey(item.getKey()));
                    //update
                    topicConfigTable.putAll(newTopicConfigTable);

                    this.brokerController.getTopicQueueMappingManager().persist();
                }
                LOGGER.info("Update slave topic config from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak == null || masterAddrBak.equals(brokerController.getBrokerAddr())) {
            return;
        }

        try {
            // Record the start time of this sync round. We use this
            // as the upper bound watermark for the next incremental
            // window so that updates happening after the snapshot is
            // fetched but before this method returns will still be
            // included in the next sync, without requiring an
            // excessively large safety gap.
            long syncStartTime = System.currentTimeMillis();

            // Collect all topics known on this broker (after topic config sync).
            ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
            if (topicConfigTable == null || topicConfigTable.isEmpty()) {
                return;
            }

            List<String> allTopics = new ArrayList<>(topicConfigTable.keySet());
            int batchSize = this.brokerController.getBrokerConfig().getSyncConsumerOffsetBatchTopicNum();
            if (batchSize <= 0) {
                batchSize = 100;
            }

            long sinceBase = this.lastConsumerOffsetSyncTimestamp;
            long safeGap = this.brokerController.getBrokerConfig().getSyncConsumerOffsetSafeGapMillis();
            if (safeGap < 0) {
                safeGap = 0;
            }
            // Apply a small overlap window on time-based incremental
            // sync to avoid missing updates around the boundary due
            // to network delay or clock skew between master and
            // slave. The trade-off is that a small portion of offsets
            // might be fetched more than once, which is acceptable
            // for this idempotent merge behavior.
            long since = sinceBase > safeGap ? sinceBase - safeGap : 0L;
            ConsumerOffsetManager consumerOffsetManager = this.brokerController.getConsumerOffsetManager();

            for (int i = 0; i < allTopics.size(); i += batchSize) {
                int end = Math.min(i + batchSize, allTopics.size());
                List<String> batchTopics = allTopics.subList(i, end);

                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getConsumerOffsetByTopicBatch(masterAddrBak, batchTopics, since);
                if (offsetWrapper == null || offsetWrapper.getOffsetTable() == null || offsetWrapper.getOffsetTable().isEmpty()) {
                    continue;
                }

                // Backward compatibility for old masters:
                // Old masters do not understand topic / time based
                // incremental parameters and always return the full
                // offset snapshot. In that case, treating the result
                // as incremental would still be logically correct but
                // unnecessarily expensive because we keep merging a
                // large offset table. Here we check whether the
                // response contains topics beyond the requested batch
                // to detect such old masters:
                // - New master: only returns offsets related to
                //   requested topics (and their LMQ children).
                // - Old master: returns offsets for all topics.
                if (isFullOffsetSnapshotFromOldMaster(offsetWrapper, batchTopics)) {
                    // Use the full snapshot from master to overwrite
                    // local offsets once, then stop this sync loop.
                    consumerOffsetManager.setOffsetTable(new ConcurrentHashMap<>(offsetWrapper.getOffsetTable()));
                    if (offsetWrapper.getDataVersion() != null) {
                        consumerOffsetManager.getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
                    }
                    break;
                }

                consumerOffsetManager.getOffsetTable().putAll(offsetWrapper.getOffsetTable());
                if (offsetWrapper.getDataVersion() != null) {
                    consumerOffsetManager.getDataVersion().assignNewOne(offsetWrapper.getDataVersion());
                }
            }

            consumerOffsetManager.persist();
            // Use the start time of this sync as the new watermark so
            // that the time-based incremental window does not rely on
            // the total duration of this method. Combined with the
            // safety gap, this avoids missing updates that happened
            // while this sync was in progress.
            this.lastConsumerOffsetSyncTimestamp = syncStartTime;
            LOGGER.info("Update slave consumer offset from master incrementally, master={}, topics={}, sinceTimestamp={}, safeGapMillis={}, baseTimestamp={}",
                masterAddrBak, allTopics.size(), since, safeGap, sinceBase);
        } catch (Exception e) {
            LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
        }
    }

    /**
     * Detect whether the given wrapper from master is effectively a
     * full offset snapshot instead of a topic-filtered one.
     * <p>
     * New masters honor {@code topicList} / {@code sinceTimestamp}
     * and will only return offsets that "belong to" the requested
     * {@code batchTopics} (including LMQ / lite child topics whose
     * parent is in the list). Old masters ignore these parameters and
     * always return the whole offset table. We leverage this
     * difference to detect old masters and downgrade our behavior.
     */
    private boolean isFullOffsetSnapshotFromOldMaster(ConsumerOffsetSerializeWrapper wrapper, List<String> batchTopics) {
        if (wrapper == null || wrapper.getOffsetTable() == null || wrapper.getOffsetTable().isEmpty()) {
            return false;
        }
        if (batchTopics == null || batchTopics.isEmpty()) {
            // No topic filter means the caller explicitly asks for a
            // full snapshot.
            return true;
        }

        // Build topic whitelist for this batch.
        List<String> requestedTopics = new ArrayList<>(batchTopics);

        for (String topicAtGroup : wrapper.getOffsetTable().keySet()) {
            int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
            String topic = idx > 0 ? topicAtGroup.substring(0, idx) : topicAtGroup;

            boolean match = false;
            // Normal topics must appear in batchTopics.
            if (requestedTopics.contains(topic)) {
                match = true;
            } else if (MixAll.isLmq(topic)) {
                // For LMQ / lite topics, as long as the parent topic
                // appears in batchTopics, treat it as belonging to
                // this batch.
                for (String parent : requestedTopics) {
                    if (LiteUtil.belongsTo(topic, parent)) {
                        match = true;
                        break;
                    }
                }
            }

            if (!match) {
                // Found a topic that does not belong to this batch,
                // which implies master did not filter by topic and is
                // very likely an old version. Treat it as a full
                // snapshot.
                return true;
            }
        }

        // All topics are within the batch; treat as incremental
        // snapshot.
        return false;
    }

    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                String delayOffset =
                        this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    String fileName =
                            StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                                    .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        MixAll.string2File(delayOffset, fileName);
                        this.brokerController.getScheduleMessageService().loadWhenSyncDelayOffset();
                    } catch (IOException e) {
                        LOGGER.error("Persist file Exception, {}", fileName, e);
                    }
                }
                LOGGER.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                SubscriptionGroupWrapper subscriptionWrapper =
                        this.brokerController.getBrokerOuterAPI()
                                .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {
                    SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();
                    subscriptionGroupManager.getDataVersion().assignNewOne(subscriptionWrapper.getDataVersion());

                    ConcurrentMap<String, SubscriptionGroupConfig> curSubscriptionGroupTable =
                            subscriptionGroupManager.getSubscriptionGroupTable();
                    ConcurrentMap<String, SubscriptionGroupConfig> newSubscriptionGroupTable =
                            subscriptionWrapper.getSubscriptionGroupTable();
                    // delete
                    Iterator<Map.Entry<String, SubscriptionGroupConfig>> iterator = curSubscriptionGroupTable.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<String, SubscriptionGroupConfig> configEntry = iterator.next();
                        if (!newSubscriptionGroupTable.containsKey(configEntry.getKey())) {
                            iterator.remove();
                        }
                        subscriptionGroupManager.deleteSubscriptionGroupConfig(configEntry.getKey());
                    }
                    // update
                    newSubscriptionGroupTable.values().forEach(subscriptionGroupManager::putSubscriptionGroupConfig);
                    subscriptionGroupManager.updateDataVersion();
                    // persist
                    subscriptionGroupManager.persist();
                    LOGGER.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                LOGGER.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncMessageRequestMode() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                MessageRequestModeSerializeWrapper messageRequestModeSerializeWrapper =
                        this.brokerController.getBrokerOuterAPI().getAllMessageRequestMode(masterAddrBak);

                MessageRequestModeManager messageRequestModeManager =
                        this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager();
                ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> curMessageRequestModeMap =
                        messageRequestModeManager.getMessageRequestModeMap();
                ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>> newMessageRequestModeMap =
                        messageRequestModeSerializeWrapper.getMessageRequestModeMap();

                // delete
                curMessageRequestModeMap.entrySet().removeIf(e -> !newMessageRequestModeMap.containsKey(e.getKey()));
                // update
                curMessageRequestModeMap.putAll(newMessageRequestModeMap);
                // persist
                messageRequestModeManager.persist();
                LOGGER.info("Update slave Message Request Mode from master, {}", masterAddrBak);
            } catch (Exception e) {
                LOGGER.error("SyncMessageRequestMode Exception, {}", masterAddrBak, e);
            }
        }
    }

    public void syncTimerCheckPoint() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore() &&
                        !brokerController.getTimerMessageStore().isShouldRunningDequeue()) {
                    TimerCheckpoint checkpoint = this.brokerController.getBrokerOuterAPI().getTimerCheckPoint(masterAddrBak);
                    if (null != this.brokerController.getTimerCheckpoint()) {
                        this.brokerController.getTimerCheckpoint().setLastReadTimeMs(checkpoint.getLastReadTimeMs());
                        this.brokerController.getTimerCheckpoint().setMasterTimerQueueOffset(checkpoint.getMasterTimerQueueOffset());
                        this.brokerController.getTimerCheckpoint().getDataVersion().assignNewOne(checkpoint.getDataVersion());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("syncTimerCheckPoint Exception, {}", masterAddrBak, e);
            }
        }
    }

    private void syncTimerMetrics() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null) {
            try {
                if (null != brokerController.getMessageStore().getTimerMessageStore()) {
                    TimerMetrics.TimerMetricsSerializeWrapper metricsSerializeWrapper =
                            this.brokerController.getBrokerOuterAPI().getTimerMetrics(masterAddrBak);
                    if (!brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().equals(metricsSerializeWrapper.getDataVersion())) {
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getDataVersion().assignNewOne(metricsSerializeWrapper.getDataVersion());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().clear();
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().getTimingCount().putAll(metricsSerializeWrapper.getTimingCount());
                        this.brokerController.getMessageStore().getTimerMessageStore().getTimerMetrics().persist();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("SyncTimerMetrics Exception, {}", masterAddrBak, e);
            }
        }
    }
}
