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

package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

/**
 * Request header for GET_ALL_CONSUMER_OFFSET.
 * <p>
 * It is backward compatible with the legacy implementation where no
 * custom header is carried. When no {@code offsetType} is specified,
 * the broker should fall back to full offset snapshot.
 */
@RocketMQAction(value = RequestCode.GET_ALL_CONSUMER_OFFSET, resource = ResourceType.TOPIC, action = Action.GET)
public class GetAllConsumerOffsetRequestHeader implements CommandCustomHeader {

    /**
     * Optional group list for batching, separated by comma.
     */
    @CFNullable
    private String groupList;

    /**
     * Optional offset sync type. Supported values are NORMAL and LMQ.
     */
    @CFNullable
    private String offsetType;

    /**
     * Optional lower bound timestamp (inclusive) for recently updated
     * offsets. When {@code sinceTimestamp} is {@code null} or not set,
     * the broker should ignore this constraint.
     */
    @CFNullable
    private Long sinceTimestamp;

    @Override
    public void checkFields() throws RemotingCommandException {
        // nothing
    }

    public String getGroupList() {
        return groupList;
    }

    public void setGroupList(String groupList) {
        this.groupList = groupList;
    }

    public String getOffsetType() {
        return offsetType;
    }

    public void setOffsetType(String offsetType) {
        this.offsetType = offsetType;
    }

    public Long getSinceTimestamp() {
        return sinceTimestamp;
    }

    public void setSinceTimestamp(Long sinceTimestamp) {
        this.sinceTimestamp = sinceTimestamp;
    }
}
