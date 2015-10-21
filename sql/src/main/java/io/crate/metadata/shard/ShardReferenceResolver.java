/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.shard;

import com.google.common.collect.ImmutableMap;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.reference.partitioned.PartitionedColumnExpression;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;

import java.util.Map;

public class ShardReferenceResolver extends AbstractReferenceResolver {

    private final Map<ReferenceIdent, ReferenceImplementation> implementations;
    private static final ESLogger LOGGER = Loggers.getLogger(ShardReferenceResolver.class);

    @Inject
    public ShardReferenceResolver(Index index,
                                  Schemas schemas,
                                  final Map<ReferenceIdent, ReferenceImplementation> globalImplementations,
                                  final Map<ReferenceIdent, ShardReferenceImplementation> shardImplementations) {
        ImmutableMap.Builder<ReferenceIdent, ReferenceImplementation> builder = ImmutableMap.builder();
        builder.putAll(globalImplementations)
                .putAll(shardImplementations);

        if (PartitionName.isPartition(index.name())) {
            TableIdent tableIdent = new TableIdent(PartitionName.schemaName(index.name()), PartitionName.tableName(index.name()));
            try {
                DocTableInfo info = (DocTableInfo) schemas.getTableInfo(tableIdent);
                if (!schemas.isOrphanedAlias(info)) {
                    assert info.isPartitioned();
                    int i = 0;
                    int numPartitionedColumns = info.partitionedByColumns().size();

                    PartitionName partitionName;
                    try {
                        partitionName = PartitionName.fromString(index.name(), tableIdent.schema(), tableIdent.name());
                        assert partitionName.values().size() ==
                               numPartitionedColumns : "invalid number of partitioned columns";
                        for (ReferenceInfo partitionedInfo : info.partitionedByColumns()) {
                            builder.put(partitionedInfo.ident(), new PartitionedColumnExpression(
                                    partitionedInfo,
                                    partitionName.values().get(i)
                            ));
                            i++;
                        }
                    } catch (IllegalArgumentException e) {
                        LOGGER.error("Unable to load PARTITIONED BY columns from partition {}", e, index.name());
                    }
                } else {
                    LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, tableIdent.fqn());
                }
            } catch (TableUnknownException e) {
                LOGGER.error("Orphaned partition '{}' with missing table '{}' found", index, tableIdent.fqn());
            }
        }
        this.implementations = builder.build();
    }

    @Override
    protected Map<ReferenceIdent, ReferenceImplementation> implementations() {
        return implementations;
    }

}
