/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING =
            Setting.boolSetting("cluster.routing.use_adaptive_replica_selection", true,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    private List<String> awarenessAttributes;

    /**
     * 是否使用自适应的备份分片选择，默认false
     */
    private boolean useAdaptiveReplicaSelection;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    /**
     * 得到要操作的分片
     *
     */
    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    /**
     * 获取分片迭代器，就是有哪些分片会用到
     *
     */
    public ShardIterator getShards(ClusterState clusterState, String index, String id, @Nullable String routing,
                                   @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, id, routing), clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(), preference, null, null);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(indexShard, clusterState.nodes().getLocalNodeId(), clusterState.nodes(),
            preference, null, null);
    }

    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null);
    }


    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference,
                                                           @Nullable ResponseCollectorService collectorService,
                                                           @Nullable Map<String, Long> nodeCounts) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(shard,
                    clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference, collectorService, nodeCounts);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return new GroupShardsIterator<>(new ArrayList<>(set));
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState, String[] concreteIndices,
                                                              @Nullable Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetaData indexMetaData = indexMetaData(clusterState, index);
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetaData.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        set.add(RoutingTable.shardRoutingTable(indexRouting, calculateScaledShardId(indexMetaData, r, partitionOffset)));
                    }
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    /**
     * 处理preference 指定的分片
     *
     * @param indexShard       路由指定的分片, 一个分片可能包含一个primary和多个replica
     * @param localNodeId      本地节点id
     * @param nodes            所有节点
     * @param preference       偏好
     */
    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId,
                                                        DiscoveryNodes nodes, @Nullable String preference,
                                                        @Nullable ResponseCollectorService collectorService,
                                                        @Nullable Map<String, Long> nodeCounts) {
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
        }
        // 如果preference 以 '_' 起始
        if (preference.charAt(0) == '_') {
            // _prefer_nodes:1 , 取冒号前的字符串来确定是哪个类型
            Preference preferenceType = Preference.parse(preference);

            // 先查看是否制定哪个分片, 如果指定分片，那么后续就是在此分片内部指定Primary或者那个Replica
            // 比如 preference=_shards:1,2|_replica
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                // _shards:1
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                }
                // _shards:1,2|_replica 优先取1,2
                else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    // 如果preference 指定的分片在 routing 指定分片集合(primary, replicas) 内, 则找到指定节点
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                // 如果指定的Shard的 序号 不存在此索引分片集合(primary,replicas)中
                if (!found) {
                    return null;
                }
                // no more preference,  _shards:| ,未指定哪个shard
                if (index == -1 || index == preference.length() - 1) {
                    return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
                } else {
                    // update the preference and continue, 截取 _shards:1,2|3  后面的3, 也可能是 _shards:1,2|_replica
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                case PREFER_NODES:
                    final Set<String> nodesIds =
                            Arrays.stream(
                                    preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                            ).collect(Collectors.toSet());
                    // 优先此Node里的此索引下的Shard, 如果不存在那么也可用其他Node下的Shard
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    // 和上面类似,优先在当前Node上执行，如果当前Node没有此Shard, 那么可用其他的Node
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case ONLY_LOCAL:
                    // 仅在当前Node执行, 如果当前Node不存在此Index 的 Shard, 那么返回空集合
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    // 仅在指定的Node上执行, 如果那个Node上不存在Shard, 抛出异常
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index  如果 preference 不是以 "_" 开头
        int routingHash = Murmur3HashFunction.hash(preference);
        if (nodes.getMinNodeVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            // The AllocationService lists shards in a fixed order based on nodes
            // so earlier versions of this class would have a tendency to
            // select the same node across different shardIds.
            // Better overall balancing can be achieved if each shardId opts
            // for a different element in the list by also incorporating the
            // shard ID into the hash of the user-supplied preference key.
            routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        }
        if (awarenessAttributes.isEmpty()) {
            // 取指定Shard下从指定个位置开始的集合, 也就是preference值相同，每次都从同一个Node开始
            return indexShard.activeInitializingShardsIt(routingHash);
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, routingHash);
        }
    }

    private ShardIterator shardRoutings(IndexShardRoutingTable indexShard, DiscoveryNodes nodes,
            @Nullable ResponseCollectorService collectorService, @Nullable Map<String, Long> nodeCounts) {
        if (awarenessAttributes.isEmpty()) {
            // 默认false
            if (useAdaptiveReplicaSelection) {
                return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
            } else {
                // 返回一个简单的循环遍历的分片列表, 遍历起点每次+1, 到末位重置成起点
                return indexShard.activeInitializingShardsRandomIt();
            }
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
        }
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    protected IndexMetaData indexMetaData(ClusterState clusterState, String index) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetaData;
    }

    /**
     * 获取指定索引的路由列表
     *
     * @param clusterState 状态
     * @param index 索引名称
     * @param id           请求id
     * @param routing      路由规则
     */
    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        // 获取分片ID, 优先使用routing 的hash来确定, routing 不存在的话用requestId
        int shardId = generateShardId(indexMetaData(clusterState, index), id, routing);
        // 返回指定分片
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetaData indexMetaData = indexMetaData(clusterState, index);
        return new ShardId(indexMetaData.getIndex(), generateShardId(indexMetaData, id, routing));
    }

    /**
     * 生成分片id
     *
     * @param id            请求id
     */
    public static int generateShardId(IndexMetaData indexMetaData, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert(indexMetaData.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }
        // 如果可路由的分区数量 != 1
        if (indexMetaData.isRoutingPartitionedIndex()) {
            // partitionOffset == hash(id) % partitionSize, 指定Shard里replica的序号
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetaData.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetaData, effectiveRouting, partitionOffset);
    }

    /**
     * 计算分片id
     *
     */
    private static int calculateScaledShardId(IndexMetaData indexMetaData, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetaData.getRoutingNumShards()) / indexMetaData.getRoutingFactor();
    }

}
