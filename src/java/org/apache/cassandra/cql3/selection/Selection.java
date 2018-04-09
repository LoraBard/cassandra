/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class Selection
{
    /**
     * A predicate that returns <code>true</code> for static columns.
     */
    private static final Predicate<ColumnDefinition> STATIC_COLUMN_FILTER = new Predicate<ColumnDefinition>()
    {
        public boolean apply(ColumnDefinition def)
        {
            return def.isStatic();
        }
    };

    final CFMetaData cfm;
    final List<ColumnDefinition> columns;
    final SelectionColumnMapping columnMapping;
    final ResultSet.ResultMetadata metadata;
    final boolean collectTimestamps;
    final boolean collectTTLs;

    protected Selection(CFMetaData cfm,
                        List<ColumnDefinition> columns,
                        SelectionColumnMapping columnMapping,
                        boolean collectTimestamps,
                        boolean collectTTLs)
    {
        this.cfm = cfm;
        this.columns = columns;
        this.columnMapping = columnMapping;
        this.metadata = new ResultSet.ResultMetadata(columnMapping.getColumnSpecifications());
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
    }

    // Overriden by SimpleSelection when appropriate.
    public boolean isWildcard()
    {
        return false;
    }

    /**
     * Checks if this selection contains static columns.
     * @return <code>true</code> if this selection contains static columns, <code>false</code> otherwise;
     */
    public boolean containsStaticColumns()
    {
        if (cfm.isStaticCompactTable() || !cfm.hasStaticColumns())
            return false;

        if (isWildcard())
            return true;

        return !Iterables.isEmpty(Iterables.filter(columns, STATIC_COLUMN_FILTER));
    }

    /**
     * Checks if this selection contains only static columns.
     * @return <code>true</code> if this selection contains only static columns, <code>false</code> otherwise;
     */
    public boolean containsOnlyStaticColumns()
    {
        if (!containsStaticColumns())
            return false;

        if (isWildcard())
            return false;

        for (ColumnDefinition def : getColumns())
        {
            if (!def.isPartitionKey() && !def.isStatic())
                return false;
        }

        return true;
    }

    public ResultSet.ResultMetadata getResultMetadata(boolean isJson)
    {
        if (!isJson)
            return metadata;

        ColumnSpecification firstColumn = metadata.names.get(0);
        ColumnSpecification jsonSpec = new ColumnSpecification(firstColumn.ksName, firstColumn.cfName, Json.JSON_COLUMN_ID, UTF8Type.instance);
        return new ResultSet.ResultMetadata(Arrays.asList(jsonSpec));
    }

    public static Selection wildcard(CFMetaData cfm)
    {
        List<ColumnDefinition> all = new ArrayList<>(cfm.allColumns().size());
        Iterators.addAll(all, cfm.allColumnsInSelectOrder());
        return new SimpleSelection(cfm, all, true);
    }

    public static Selection wildcardWithGroupBy(CFMetaData cfm, VariableSpecifications boundNames)
    {
        List<RawSelector> rawSelectors = new ArrayList<>(cfm.allColumns().size());
        Iterator<ColumnDefinition> iter = cfm.allColumnsInSelectOrder();
        while (iter.hasNext())
        {
            ColumnDefinition.Raw raw = ColumnDefinition.Raw.forColumn(iter.next());
            rawSelectors.add(new RawSelector(raw, null));
        }
        return fromSelectors(cfm, rawSelectors, boundNames, true);
    }

    public static Selection forColumns(CFMetaData cfm, List<ColumnDefinition> columns)
    {
        return new SimpleSelection(cfm, columns, false);
    }

    public int addColumnForOrdering(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    public void addFunctionsTo(List<Function> functions)
    {
    }

    private static boolean processesSelection(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
                return true;
        }
        return false;
    }

    public static Selection fromSelectors(CFMetaData cfm, List<RawSelector> rawSelectors, VariableSpecifications boundNames, boolean hasGroupBy)
    {
        List<ColumnDefinition> defs = new ArrayList<>();

        SelectorFactories factories =
                SelectorFactories.createFactoriesAndCollectColumnDefinitions(RawSelector.toSelectables(rawSelectors, cfm), null, cfm, defs, boundNames);
        SelectionColumnMapping mapping = collectColumnMappings(cfm, rawSelectors, factories);

        return (processesSelection(rawSelectors) || rawSelectors.size() != defs.size() || hasGroupBy)
               ? new SelectionWithProcessing(cfm, defs, mapping, factories)
               : new SimpleSelection(cfm, defs, mapping, false);
    }

    /**
     * Returns the index of the specified column within the resultset
     * @param c the column
     * @return the index of the specified column within the resultset or -1
     */
    public int getResultSetIndex(ColumnDefinition c)
    {
        return getColumnIndex(c);
    }

    /**
     * Returns the index of the specified column
     * @param c the column
     * @return the index of the specified column or -1
     */
    protected final int getColumnIndex(ColumnDefinition c)
    {
        for (int i = 0, m = columns.size(); i < m; i++)
            if (columns.get(i).name.equals(c.name))
                return i;
        return -1;
    }

    private static SelectionColumnMapping collectColumnMappings(CFMetaData cfm,
                                                                List<RawSelector> rawSelectors,
                                                                SelectorFactories factories)
    {
        SelectionColumnMapping selectionColumns = SelectionColumnMapping.newMapping();
        Iterator<RawSelector> iter = rawSelectors.iterator();
        for (Selector.Factory factory : factories)
        {
            ColumnSpecification colSpec = factory.getColumnSpecification(cfm);
            ColumnIdentifier alias = iter.next().alias;
            factory.addColumnMapping(selectionColumns,
                                     alias == null ? colSpec : colSpec.withAlias(alias));
        }
        return selectionColumns;
    }

    protected abstract Selectors newSelectors(QueryOptions options) throws InvalidRequestException;

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public List<ColumnDefinition> getColumns()
    {
        return columns;
    }

    /**
     * @return the mappings between resultset columns and the underlying columns
     */
    public SelectionColumns getColumnMapping()
    {
        return columnMapping;
    }

    public abstract boolean isAggregate();

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("columns", columns)
                          .add("columnMapping", columnMapping)
                          .add("metadata", metadata)
                          .add("collectTimestamps", collectTimestamps)
                          .add("collectTTLs", collectTTLs)
                          .toString();
    }

    public static List<ByteBuffer> rowToJson(List<ByteBuffer> row, ProtocolVersion protocolVersion, ResultSet.ResultMetadata metadata)
    {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < metadata.names.size(); i++)
        {
            if (i > 0)
                sb.append(", ");

            ColumnSpecification spec = metadata.names.get(i);
            String columnName = spec.name.toString();
            if (!columnName.equals(columnName.toLowerCase(Locale.US)))
                columnName = "\"" + columnName + "\"";

            ByteBuffer buffer = row.get(i);
            sb.append('"');
            sb.append(Json.quoteAsJsonString(columnName));
            sb.append("\": ");
            if (buffer == null)
                sb.append("null");
            else if (!buffer.hasRemaining())
                sb.append("\"\"");
            else
                sb.append(spec.type.toJSONString(buffer, protocolVersion));
        }
        sb.append("}");
        return Collections.singletonList(UTF8Type.instance.getSerializer().serialize(sb.toString()));
    }


    static interface Selectors
    {
        public boolean isAggregate();

        /**
         * Adds the current row of the specified <code>ResultSetBuilder</code>.
         *
         * @param protocolVersion
         * @param rs the <code>ResultSetBuilder</code>
         * @throws InvalidRequestException
         */
        public void addInputRow(ProtocolVersion protocolVersion, ResultBuilder rs) throws InvalidRequestException;

        public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion) throws InvalidRequestException;

        public void reset();
    }

    // Special cased selection for when only columns are selected.
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(CFMetaData cfm, List<ColumnDefinition> columns, boolean isWildcard)
        {
            this(cfm, columns, SelectionColumnMapping.simpleMapping(columns), isWildcard);
        }

        public SimpleSelection(CFMetaData cfm,
                               List<ColumnDefinition> columns,
                               SelectionColumnMapping metadata,
                               boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(cfm, columns, metadata, false, false);
            this.isWildcard = isWildcard;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }

        public boolean isAggregate()
        {
            return false;
        }

        protected Selectors newSelectors(QueryOptions options)
        {
            return new Selectors()
            {
                private List<ByteBuffer> current;

                public void reset()
                {
                    current = null;
                }

                public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion)
                {
                    return current;
                }

                public void addInputRow(ProtocolVersion protocolVersion, ResultBuilder rs) throws InvalidRequestException
                {
                    current = rs.current;
                }

                public boolean isAggregate()
                {
                    return false;
                }
            };
        }
    }

    private static class SelectionWithProcessing extends Selection
    {
        private final SelectorFactories factories;

        public SelectionWithProcessing(CFMetaData cfm,
                                       List<ColumnDefinition> columns,
                                       SelectionColumnMapping metadata,
                                       SelectorFactories factories) throws InvalidRequestException
        {
            super(cfm,
                  columns,
                  metadata,
                  factories.containsWritetimeSelectorFactory(),
                  factories.containsTTLSelectorFactory());

            this.factories = factories;
        }

        @Override
        public void addFunctionsTo(List<Function> functions)
        {
            factories.addFunctionsTo(functions);
        }

        @Override
        public int getResultSetIndex(ColumnDefinition c)
        {
            int index = getColumnIndex(c);

            if (index < 0)
                return -1;

            for (int i = 0, m = factories.size(); i < m; i++)
                if (factories.get(i).isSimpleSelectorFactory(index))
                    return i;

            return -1;
        }

        @Override
        public int addColumnForOrdering(ColumnDefinition c)
        {
            int index = super.addColumnForOrdering(c);
            factories.addSelectorForOrdering(c, index);
            return factories.size() - 1;
        }

        public boolean isAggregate()
        {
            return factories.doesAggregation();
        }

        protected Selectors newSelectors(final QueryOptions options) throws InvalidRequestException
        {
            return new Selectors()
            {
                private final List<Selector> selectors = factories.newInstances(options);

                public void reset()
                {
                    for (Selector selector : selectors)
                        selector.reset();
                }

                public boolean isAggregate()
                {
                    return factories.doesAggregation();
                }

                public List<ByteBuffer> getOutputRow(ProtocolVersion protocolVersion) throws InvalidRequestException
                {
                    List<ByteBuffer> outputRow = new ArrayList<>(selectors.size());

                    for (Selector selector: selectors)
                        outputRow.add(selector.getOutput(protocolVersion));

                    return outputRow;
                }

                public void addInputRow(ProtocolVersion protocolVersion, ResultBuilder rs) throws InvalidRequestException
                {
                    for (Selector selector : selectors)
                        selector.addInput(protocolVersion, rs);
                }
            };
        }

    }
}
