/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.collectors;

import io.crate.action.job.TransportJobAction;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.data.RowConsumer;
import io.crate.exceptions.TableUnknownException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.executor.transport.kill.TransportKillJobsNodeAction;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.ShardCollectorProviderFactory;
import io.crate.planner.distribution.DistributionInfo;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IllegalShardRoutingStateException;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ShardStateAwareRemoteCollector implements CrateCollector {

    private static final Logger LOGGER = Loggers.getLogger(ShardStateAwareRemoteCollector.class);
    private static final int SENDER_PHASE_ID = 0;

    private final UUID jobId;
    private final String localNode;
    private final TransportJobAction transportJobAction;
    private final TransportKillJobsNodeAction transportKillJobsNodeAction;
    private final JobContextService jobContextService;
    private final JobCollectContext jobCollectContext;
    private final RowConsumer consumer;
    private final Object killLock = new Object();

    private final boolean scrollRequired;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final int shardId;
    private final String indexName;
    private final RoutedCollectPhase originalCollectPhase;
    private final ShardCollectorProviderFactory shardCollectorProviderFactory;
    private final IndicesService indicesService;
    private boolean collectorKilled = false;
    private CrateCollector collector;

    public ShardStateAwareRemoteCollector(UUID jobId,
                                          String indexName,
                                          int shardId,
                                          TransportJobAction transportJobAction,
                                          TransportKillJobsNodeAction transportKillJobsNodeAction,
                                          JobContextService jobContextService,
                                          JobCollectContext jobCollectContext,
                                          RowConsumer consumer,
                                          RoutedCollectPhase originalCollectPhase,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          IndicesService indicesService,
                                          ShardCollectorProviderFactory shardCollectorProviderFactory) {
        this.jobId = jobId;
        this.indexName = indexName;
        this.shardId = shardId;
        this.scrollRequired = consumer.requiresScroll();
        this.transportJobAction = transportJobAction;
        this.transportKillJobsNodeAction = transportKillJobsNodeAction;
        this.jobContextService = jobContextService;
        this.jobCollectContext = jobCollectContext;
        this.consumer = consumer;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.originalCollectPhase = originalCollectPhase;
        this.localNode = clusterService.localNode().getId();
        this.indicesService = indicesService;
        this.shardCollectorProviderFactory = shardCollectorProviderFactory;
    }

    @Override
    public void doCollect() {
        ClusterState clusterState = clusterService.state();
        checkStateAndCollect(clusterState);
    }

    private void checkStateAndCollect(ClusterState clusterState) {
        IndexShardRoutingTable shardRoutings = clusterState.routingTable().shardRoutingTable(indexName, shardId);
        // for update operations primaryShards must be used
        // (for others that wouldn't be the case, but at this point it is not easily visible which is the case)
        ShardRouting shardRouting = shardRoutings.primaryShard();
        String remoteNode = shardRouting.currentNodeId();
        if (localNode.equals(remoteNode) == false) {
            RoutedCollectPhase remoteCollectPhase =
                createRemoteCollectPhase(jobId, originalCollectPhase, indexName, shardId, remoteNode);
            synchronized (killLock) {
                if (collectorKilled == false) {
                    collector = new RemoteCollector(jobId, localNode, remoteNode, transportJobAction, transportKillJobsNodeAction,
                        jobContextService, jobCollectContext.queryPhaseRamAccountingContext(), consumer, remoteCollectPhase);
                } else {
                    consumer.accept(null, new InterruptedException());
                }
            }
            // collect outside the synchronized block
            if (collector != null) {
                collector.doCollect();
            }
        } else {
            if (shardRouting.started() == true) {
                // The shard is on the local node, so don't create a remote collector but a local one.
                // We might've arrived here as a result of the cluster state observer detecting the shard relocated, in
                // which case the ClusterState-Update thread is running this.
                // The creation of a ShardCollectorProvider accesses the clusterState, which leads to an assertionError
                // if accessed within a ClusterState-Update thread.

                threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                    IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
                    if (indexMetaData != null) {
                        IndexService indexShards = indicesService.indexService(indexMetaData.getIndex());
                        try {
                            synchronized (killLock) {
                                if (collectorKilled == false) {
                                    collector = shardCollectorProviderFactory.create(indexShards.getShard(shardId)).
                                        getCollectorBuilder(originalCollectPhase, scrollRequired, jobCollectContext).
                                        build(consumer);
                                } else {
                                    consumer.accept(null, new InterruptedException());
                                }
                            }
                            // collect outside the synchronized block
                            if (collector != null) {
                                collector.doCollect();
                            }
                        } catch (Exception e) {
                            consumer.accept(null, e);
                        }
                    } else {
                        consumer.accept(null, new TableUnknownException(TableIdent.fromIndexName(indexName)));
                    }
                });
            } else {
                LOGGER.warn("Unable to collect from a shard that's not started. Waiting for primary shard {} to start.",
                    shardRouting.shardId());

                ClusterStateObserver clusterStateObserver = new ClusterStateObserver(clusterState, clusterService,
                    new TimeValue(60000), LOGGER, threadPool.getThreadContext());
                clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        checkStateAndCollect(state);
                    }

                    @Override
                    public void onClusterServiceClose() {

                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        consumer.accept(null, new IllegalShardRoutingStateException(shardRouting,
                            "Timed out waiting for shard to start"));
                    }

                }, newState -> newState.routingTable().shardRoutingTable(indexName, shardId).primaryShard().started());
            }
        }
    }

    private RoutedCollectPhase createRemoteCollectPhase(
        UUID childJobId, RoutedCollectPhase collectPhase, String index, Integer shardId, String nodeId) {

        Routing routing = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(nodeId,
            TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(index, Collections.singletonList(shardId)).map()).map());
        return new RoutedCollectPhase(
            childJobId,
            SENDER_PHASE_ID,
            collectPhase.name(),
            routing,
            collectPhase.maxRowGranularity(),
            collectPhase.toCollect(),
            new ArrayList<>(Projections.shardProjections(collectPhase.projections())),
            collectPhase.whereClause(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
        synchronized (killLock) {
            collectorKilled = true;
            if (collector != null) {
                collector.kill(throwable);
            }
        }
    }
}
