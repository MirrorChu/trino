/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column

    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final boolean updateTarget;
    private final Optional<Boolean> useConnectorNodePartitioning;

    // We need this factory method to disambiguate with the constructor used for deserializing
    // from a json object. The deserializer sets some fields which are never transported
    // to null
    public static TableScanNode newInstance(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            boolean updateTarget,
            Optional<Boolean> useConnectorNodePartitioning)
    {
        return new TableScanNode(id, table, outputs, assignments, TupleDomain.all(), updateTarget, useConnectorNodePartitioning);
    }

    /*
     * This constructor is for JSON deserialization only. Do not use.
     * It's marked as @Deprecated to help avoid usage, and not because we plan to remove it.
     */
    @Deprecated
    @JsonCreator
    public TableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("updateTarget") boolean updateTarget,
            @JsonProperty("useConnectorNodePartitioning") Optional<Boolean> useConnectorNodePartitioning)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        this.enforcedConstraint = null;
        this.updateTarget = updateTarget;
        this.useConnectorNodePartitioning = requireNonNull(useConnectorNodePartitioning, "useConnectorNodePartitioning is null");
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TupleDomain<ColumnHandle> enforcedConstraint,
            boolean updateTarget,
            Optional<Boolean> useConnectorNodePartitioning)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.updateTarget = updateTarget;
        this.useConnectorNodePartitioning = requireNonNull(useConnectorNodePartitioning, "useConnectorNodePartitioning is null");
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    /**
     * A TupleDomain that represents a predicate that has been successfully pushed into
     * this TableScan node. In other words, predicates that were removed from filters
     * above the TableScan node because the TableScan node can guarantee it.
     * <p>
     * This field is used to make sure that predicates which were previously pushed down
     * do not get lost in subsequent refinements of the table layout.
     */
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        // enforcedConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(enforcedConstraint != null, "enforcedConstraint should only be used in planner. It is not transported to workers.");
        return enforcedConstraint;
    }

    @JsonProperty("updateTarget")
    public boolean isUpdateTarget()
    {
        return updateTarget;
    }

    @JsonProperty("useConnectorNodePartitioning")
    public Optional<Boolean> getUseConnectorNodePartitioning()
    {
        return useConnectorNodePartitioning;
    }

    public boolean isUseConnectorNodePartitioning()
    {
        return useConnectorNodePartitioning
                .orElseThrow(() -> new VerifyException("useConnectorNodePartitioning is not present"));
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("outputSymbols", outputSymbols)
                .add("assignments", assignments)
                .add("enforcedConstraint", enforcedConstraint)
                .add("updateTarget", updateTarget)
                .toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    public TableScanNode withUseConnectorNodePartitioning(boolean useConnectorNodePartitioning)
    {
        return new TableScanNode(
                getId(),
                table,
                outputSymbols,
                assignments,
                enforcedConstraint,
                updateTarget,
                Optional.of(useConnectorNodePartitioning));
    }
}
