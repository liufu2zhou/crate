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

package io.crate.operation.collect;

import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.JobContextService;
import io.crate.operation.collect.collectors.RemoteCollector;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import io.crate.operation.collect.collectors.ShardStateAwareRemoteCollector;

import java.util.UUID;

/**
 * Used to create RemoteCollectors
 * Those RemoteCollectors act as proxy collectors to collect data from a shard located on another node.
 */
@Singleton
public class RemoteCollectorFactory {

    private final ClusterService clusterService;
    private final JobContextService jobContextService;
    private final TransportActionProvider transportActionProvider;
    private final ThreadPool threadPool;
    private final IndicesService indicesService;

    @Inject
    public RemoteCollectorFactory(ClusterService clusterService,
                                  JobContextService jobContextService,
                                  TransportActionProvider transportActionProvider,
                                  IndicesService indicesService,
                                  ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.jobContextService = jobContextService;
        this.transportActionProvider = transportActionProvider;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
    }

    /**
     * create a RemoteCollector
     * The RemoteCollector will collect data from another node using a wormhole as if it was collecting on this node.
     * <p>
     * This should only be used if a shard is not available on the current node due to a relocation
     */
    public CrateCollector.Builder createCollector(String index,
                                                  Integer shardId,
                                                  RoutedCollectPhase collectPhase,
                                                  JobCollectContext jobCollectContext,
                                                  ShardCollectorProviderFactory shardCollectorProviderFactory) {
        final UUID childJobId = UUID.randomUUID(); // new job because subContexts can't be merged into an existing job

        return consumer -> new ShardStateAwareRemoteCollector(
            childJobId,
            index,
            shardId,
            transportActionProvider.transportJobInitAction(),
            transportActionProvider.transportKillJobsNodeAction(),
            jobContextService,
            jobCollectContext,
            consumer,
            collectPhase,
            clusterService,
            threadPool,
            indicesService,
            shardCollectorProviderFactory);
    }
}
