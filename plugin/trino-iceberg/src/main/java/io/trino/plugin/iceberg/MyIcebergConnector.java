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
 * @author XU Boqing
 * @version 1.3.0
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.SERIALIZABLE;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class MyIcebergConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final IcebergTransactionManager transactionManager;
    private final IcebergMetadataFactory metadataFactory;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;
    private final Set<SystemTable> systemTables;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final ConnectorAccessControl accessControl;
    private final Set<Procedure> procedures;

    public MyIcebergConnector(
            LifeCycleManager lifeCycleManager,
            IcebergTransactionManager transactionManager,
            IcebergMetadataFactory metadataFactory,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider nodePartitioningProvider,
            Set<SystemTable> systemTables,
            List<PropertyMetadata<?>> sessionProperties,
            List<PropertyMetadata<?>> schemaProperties,
            List<PropertyMetadata<?>> tableProperties,
            ConnectorAccessControl accessControl,
            Set<Procedure> procedures)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.schemaProperties = ImmutableList.copyOf(requireNonNull(schemaProperties, "schemaProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = requireNonNull(procedures, "procedures is null");
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return Optional.of(new IcebergHandleResolver()) (in Optional<ConnectorHandleResolver> type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public Optional<ConnectorHandleResolver> getHandleResolver()
    {
        return Optional.of(new IcebergHandleResolver());
    }

    /**
     * This method is used to tell the user the raptor's property -- if it is single-statement-writes-only
     * @return The result of the test
     * @see org.assertj.core.api.Assertions.assertThat
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return true;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }

    /**
     * This method is a get method
     * @param transaction in ConnectorTransactionHandle
     * @return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader()) (in ConnectorMetadata type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return splitManager (in ConnectorSplitManager type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return pageSourceProvider (in ConnectorPageSourceProvider type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return pageSinkProvider (in ConnectorPageSinkProvider type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return nodePartitioningProvider (in ConnectorNodePartitioningProvider type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return systemTables
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return procedures
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return sessionProperties
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return schemaProperties
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return tableProperties
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return tableProperties
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return tableProperties;
    }

    /**
     * This method is a get method
     * @param no parameter is required
     * @return accessControl
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl;
    }

    /**
     * This method set the initial transaction of thie RaptorConnector
     * @param isolationLevel in IsolationLevel
     * @param readOnly in boolean
     * @return transaction (in ConnectorTransactionHandle type)
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(SERIALIZABLE, isolationLevel);
        ConnectorTransactionHandle transaction = new HiveTransactionHandle();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            transactionManager.put(transaction, metadataFactory.create());
        }
        return transaction;
    }

    /**
     * This method is used after setting the initial transaction of thie RaptorConnector and is commit the transaction
     * @param transaction (in ConnectorTransactionHandle type)
     * @return this is a void method
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.remove(transaction);
    }

    /**
     * This method shutdown the whole process
     * @param no parameter required
     * @return this is a void method
     */
    //CS304 Issue link: https://github.com/trinodb/trino/issues/7107
    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
