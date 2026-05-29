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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    /**
     * Groups that had consumer offsets returned by master in the last
     * successful {@link #syncConsumerOffset()} round. When cleaning
     * offsets for deleted subscription groups on slave, we use this as
     * a soft guardrail: if a group still appeared in the most recent
     * offset sync result from master, we skip removing its local
     * offsets for now to avoid prematurely dropping offsets that are
     * still present on master.
     */
    private volatile Set<String> lastSyncedConsumerOffsetGroups = Collections.emptySet();

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

            ConcurrentMap<String, TopicConfig> topicConfigTable = this.brokerController.getTopicConfigManager().getTopicConfigTable();
            if (topicConfigTable == null) {
                return;
            }

            int batchSize = this.brokerController.getBrokerConfig().getSyncConsumerOffsetBatchNum();
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
            // Track groups that have offsets returned by master in
            // this sync round, for use as a best-effort guard when
            // cleaning group offsets on slave.
            Set<String> syncedGroupsThisRound = new HashSet<>();

            // Clean deleted topics against the local table before the normal
            // full snapshot replacement. Otherwise deleted normal topics would
            // disappear from offsetTable after setOffsetTable(), making it
            // impossible to call cleanOffsetByTopic() for related reset / pull
            // offset tables.
            cleanupOrphanConsumerOffsets(consumerOffsetManager, topicConfigTable);

            // Normal-topic offsets are small and mirrored in memory on master.
            // Fetch them as a full normal-only snapshot every round to keep the
            // original full-sync semantics without mixing in massive LMQ data.
            ConsumerOffsetSerializeWrapper normalOffsetWrapper =
                this.brokerController.getBrokerOuterAPI().getNormalConsumerOffset(masterAddrBak);
            if (normalOffsetWrapper != null && normalOffsetWrapper.getOffsetTable() != null) {
                replaceNormalOffsets(consumerOffsetManager, normalOffsetWrapper.getOffsetTable());
                collectOffsetGroupsFromWrapper(normalOffsetWrapper, syncedGroupsThisRound);
                if (normalOffsetWrapper.getDataVersion() != null) {
                    consumerOffsetManager.getDataVersion().assignNewOne(normalOffsetWrapper.getDataVersion());
                }
            }

            ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
                this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();
            List<String> allGroups = subscriptionGroupTable == null ? Collections.emptyList() : new ArrayList<>(subscriptionGroupTable.keySet());
            for (int i = 0; i < allGroups.size(); i += batchSize) {
                int end = Math.min(i + batchSize, allGroups.size());
                List<String> batchGroups = allGroups.subList(i, end);

                ConsumerOffsetSerializeWrapper lmqOffsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getLmqConsumerOffsetByGroupBatch(masterAddrBak, batchGroups, since);
                if (lmqOffsetWrapper == null || lmqOffsetWrapper.getOffsetTable() == null || lmqOffsetWrapper.getOffsetTable().isEmpty()) {
                    continue;
                }

                collectOffsetGroupsFromWrapper(lmqOffsetWrapper, syncedGroupsThisRound);
                mergeOffsetWrapper(consumerOffsetManager, lmqOffsetWrapper);
                if (lmqOffsetWrapper.getDataVersion() != null) {
                    consumerOffsetManager.getDataVersion().assignNewOne(lmqOffsetWrapper.getDataVersion());
                }
            }

            // Update guardrail view for subscription-group cleanup to
            // reflect what master returned in this sync round.
            this.lastSyncedConsumerOffsetGroups = syncedGroupsThisRound;

            consumerOffsetManager.persist();
            // Use the start time of this sync as the new watermark so
            // that the time-based incremental window does not rely on
            // the total duration of this method. Combined with the
            // safety gap, this avoids missing updates that happened
            // while this sync was in progress.
            this.lastConsumerOffsetSyncTimestamp = syncStartTime;
            LOGGER.info("Update slave consumer offset from master, master={}, normalTopics={}, lmqGroups={}, sinceTimestamp={}, safeGapMillis={}, baseTimestamp={}",
                masterAddrBak, topicConfigTable.size(), allGroups.size(), since, safeGap, sinceBase);
        } catch (Exception e) {
            LOGGER.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
        }
    }

    private void replaceNormalOffsets(ConsumerOffsetManager consumerOffsetManager,
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> normalOffsetTable) {
        ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> newOffsetTable = new ConcurrentHashMap<>();
        if (normalOffsetTable != null) {
            for (Map.Entry<String, ConcurrentMap<Integer, Long>> entry : normalOffsetTable.entrySet()) {
                String topicAtGroup = entry.getKey();
                if (topicAtGroup == null) {
                    continue;
                }
                int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
                if (idx <= 0) {
                    continue;
                }
                String topic = topicAtGroup.substring(0, idx);
                if (!MixAll.isLmq(topic)) {
                    newOffsetTable.put(topicAtGroup, entry.getValue());
                }
            }
        }
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> currentOffsetTable = consumerOffsetManager.getOffsetTable();
        if (currentOffsetTable != null) {
            for (Map.Entry<String, ConcurrentMap<Integer, Long>> entry : currentOffsetTable.entrySet()) {
                String topicAtGroup = entry.getKey();
                if (topicAtGroup == null) {
                    continue;
                }
                int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
                if (idx <= 0) {
                    continue;
                }
                String topic = topicAtGroup.substring(0, idx);
                if (MixAll.isLmq(topic)) {
                    newOffsetTable.put(topicAtGroup, entry.getValue());
                }
            }
        }
        consumerOffsetManager.setOffsetTable(newOffsetTable);
    }

    private void collectOffsetGroupsFromWrapper(ConsumerOffsetSerializeWrapper wrapper, Set<String> groups) {
        if (wrapper == null || groups == null) {
            return;
        }
        Map<String, ConcurrentMap<Integer, Long>> offsetTable = wrapper.getOffsetTable();
        if (offsetTable == null || offsetTable.isEmpty()) {
            return;
        }

        for (String topicAtGroup : offsetTable.keySet()) {
            if (topicAtGroup == null) {
                continue;
            }
            int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
            if (idx <= 0 || idx >= topicAtGroup.length() - 1) {
                continue;
            }
            String group = topicAtGroup.substring(idx + 1);
            if (!group.isEmpty()) {
                groups.add(group);
            }
        }
    }

    private void mergeOffsetWrapper(ConsumerOffsetManager consumerOffsetManager, ConsumerOffsetSerializeWrapper wrapper) {
        if (consumerOffsetManager == null || wrapper == null || wrapper.getOffsetTable() == null) {
            return;
        }
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> entry : wrapper.getOffsetTable().entrySet()) {
            String topicAtGroup = entry.getKey();
            if (topicAtGroup == null) {
                continue;
            }
            int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
            if (idx <= 0 || idx >= topicAtGroup.length() - 1) {
                continue;
            }
            String topic = topicAtGroup.substring(0, idx);
            if (!MixAll.isLmq(topic)) {
                continue;
            }
            String group = topicAtGroup.substring(idx + 1);
            for (Map.Entry<Integer, Long> offsetEntry : entry.getValue().entrySet()) {
                consumerOffsetManager.commitOffset(null, group, topic, offsetEntry.getKey(), offsetEntry.getValue());
            }
        }
    }

    /**
     * Remove local consumer offsets whose topics have already been
     * deleted on this broker. We use the current topicConfigTable as
     * the source of truth:
     * <ul>
     *   <li>If a topic (or its parent topic for LMQ / lite topics) is
     *   missing from {@code topicConfigTable}, we treat it as deleted
     *   and remove all related offsets on slave.</li>
     *   <li>If a topic still exists in {@code topicConfigTable} but
     *   happens to have no new offset updates in this incremental
     *   window, its offsets are kept.</li>
     * </ul>
     */
    private void cleanupOrphanConsumerOffsets(ConsumerOffsetManager consumerOffsetManager,
        ConcurrentMap<String, TopicConfig> topicConfigTable) {
        if (consumerOffsetManager == null || topicConfigTable == null) {
            return;
        }

        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = consumerOffsetManager.getOffsetTable();
        if (offsetTable == null || offsetTable.isEmpty()) {
            return;
        }

        Set<String> orphanTopics = new HashSet<>();
        for (String topicAtGroup : offsetTable.keySet()) {
            int idx = topicAtGroup.indexOf(ConsumerOffsetManager.TOPIC_GROUP_SEPARATOR);
            if (idx <= 0) {
                continue;
            }

            String topic = topicAtGroup.substring(0, idx);
            String topicForMatch = topic;
            // For lite topics backed by LMQ, use parent topic when
            // determining whether the topic still exists.
            if (MixAll.isLmq(topic)) {
                String parentTopic = LiteUtil.getParentTopic(topic);
                if (parentTopic != null) {
                    topicForMatch = parentTopic;
                }
            }

            if (!topicConfigTable.containsKey(topicForMatch)) {
                orphanTopics.add(topic);
            }
        }

        for (String topic : orphanTopics) {
            consumerOffsetManager.cleanOffsetByTopic(topic);
        }
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
                        String group = configEntry.getKey();
                        if (!newSubscriptionGroupTable.containsKey(group)) {
                            iterator.remove();
                            // Optionally clean local consumer offsets for
                            // groups that have been deleted on master.
                            // As a soft guardrail, if this group still
                            // appeared in the most recent consumer
                            // offset sync result from master, we skip
                            // removing its offsets for now to avoid
                            // losing offsets that are still present on
                            // master.
                            if (this.brokerController.getBrokerConfig().isCleanDeletedSubscriptionGroupOffsetInSlave()
                                && !lastSyncedConsumerOffsetGroups.contains(group)) {
                                this.brokerController.getConsumerOffsetManager().removeOffset(group);
                            }
                        }
                        subscriptionGroupManager.deleteSubscriptionGroupConfig(group);
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
