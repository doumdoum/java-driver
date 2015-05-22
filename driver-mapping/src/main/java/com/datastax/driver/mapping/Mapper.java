/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Computed;

/**
 * An object handling the mapping of a particular class.
 * <p>
 * A {@code Mapper} object is obtained from a {@code MappingManager} using the
 * {@link MappingManager#mapper} method.
 */
public class Mapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(EntityMapper.class);

    final MappingManager manager;
    final ProtocolVersion protocolVersion;
    final Class<T> klass;
    final EntityMapper<T> mapper;
    final TableMetadata tableMetadata;

    // Cache prepared statements for each type of query we use.
    private volatile Map<MapperQueryKey, PreparedStatement> preparedQueries = Collections.emptyMap();

    private static final Function<Object, Void> NOOP = Functions.constant(null);

    private volatile EnumMap<Option.Type, Option> defaultSaveOptions;
    private volatile EnumMap<Option.Type, Option> defaultGetOptions;
    private volatile EnumMap<Option.Type, Option> defaultDeleteOptions;

    private static final EnumMap<Option.Type, Option> NO_OPTIONS = new EnumMap<Option.Type, Option>(Option.Type.class);

    final Function<ResultSet, T> mapOneFunction;
    final Function<ResultSet, T> mapOneFunctionWithoutAliases;
    final Function<ResultSet, Result<T>> mapAllFunctionWithoutAliases;

    Mapper(MappingManager manager, Class<T> klass, EntityMapper<T> mapper) {
        this.manager = manager;
        this.klass = klass;
        this.mapper = mapper;

        KeyspaceMetadata keyspace = session().getCluster().getMetadata().getKeyspace(mapper.getKeyspace());
        this.tableMetadata = keyspace == null ? null : keyspace.getTable(Metadata.quote(mapper.getTable()));

        this.protocolVersion = manager.getSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum();
        this.mapOneFunction = new Function<ResultSet, T>() {
            public T apply(ResultSet rs) {
                return Mapper.this.mapAliased(rs).one();
            }
        };
        this.mapOneFunctionWithoutAliases = new Function<ResultSet, T>() {
            public T apply(ResultSet rs) {
                return Mapper.this.map(rs).one();
            }
        };
        this.mapAllFunctionWithoutAliases = new Function<ResultSet, Result<T>>() {
            public Result<T> apply(ResultSet rs) {
                return Mapper.this.map(rs);
            }
        };

        this.defaultSaveOptions = NO_OPTIONS;
        this.defaultGetOptions = NO_OPTIONS;
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    Session session() {
        return manager.getSession();
    }

    PreparedStatement getPreparedQuery(QueryType type, EnumMap<Option.Type, Option> options) {

        MapperQueryKey pqk = new MapperQueryKey(type, options);

        PreparedStatement stmt = preparedQueries.get(pqk);
        if (stmt == null) {
            synchronized (preparedQueries) {
                stmt = preparedQueries.get(pqk);
                if (stmt == null) {
                    String queryString = type.makePreparedQueryString(tableMetadata, mapper, options.values());
                    logger.debug("Preparing query {}", queryString);
                    stmt = session().prepare(queryString);
                    Map<MapperQueryKey, PreparedStatement> newQueries = new HashMap<MapperQueryKey, PreparedStatement>(preparedQueries);
                    newQueries.put(pqk, stmt);
                    preparedQueries = newQueries;
                }
            }
        }
        return stmt;
    }

    /**
     * The {@code TableMetadata} for this mapper.
     *
     * @return the {@code TableMetadata} for this mapper or {@code null} if keyspace is not set.
     */
    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    /**
     * The {@code MappingManager} managing this mapper.
     *
     * @return the {@code MappingManager} managing this mapper.
     */
    public MappingManager getManager() {
        return manager;
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity) {
        return saveQuery(entity, this.defaultSaveOptions);
    }

    /**
     * Creates a query that can be used to save the provided entity.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #save}
     * or {@link #saveAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity the entity to save.
     * @return a query that saves {@code entity} (based on it's defined mapping).
     */
    public Statement saveQuery(T entity, Option... options) {
        return saveQuery(entity, toMapWithDefaults(options, this.defaultSaveOptions));
    }

    private Statement saveQuery(T entity, EnumMap<Option.Type, Option> options) {
        BoundStatement bs = getPreparedQuery(QueryType.SAVE, options).bind();
        int i = 0;
        for (ColumnMapper<T> cm : mapper.allColumns()) {
            if (cm.kind != ColumnMapper.Kind.COMPUTED) {
                Object value = cm.getValue(entity);
                bs.setBytesUnsafe(i++, value == null ? null : cm.getDataType().serialize(value, protocolVersion));
            }
        }

        for (Option opt : options.values()) {
            if (opt.isValidFor(QueryType.SAVE))
                opt.addToPreparedStatement(bs, i++);
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Save an entity mapped by this mapper.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     */
    public void save(T entity) {
        session().execute(saveQuery(entity));
    }

    /**
     * Save an entity mapped by this mapper and using special options for save.
     * This method allows you to provide a suite of {@link Option} to include in
     * the SAVE query. Options currently supported for SAVE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Time-to-live (ttl)</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     */
    public void save(T entity, Option... options) {
        session().execute(saveQuery(entity, options));
    }

    /**
     * Save an entity mapped by this mapper asynchronously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity))}.
     *
     * @param entity the entity to save.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity) {
        return Futures.transform(session().executeAsync(saveQuery(entity)), NOOP);
    }

    /**
     * Save an entity mapped by this mapper asynchronously using special options for save.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(saveQuery(entity, options))}.
     *
     * @param entity  the entity to save.
     * @param options the options object specified defining special options when saving.
     * @return a future on the completion of the save operation.
     */
    public ListenableFuture<Void> saveAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(saveQuery(entity, options)), NOOP);
    }

    /**
     * Creates a query to fetch entity given its PRIMARY KEY.
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key).
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually,
     * but in other cases, calling {@link #get} or {@link #getAsync} is shorter.
     * <p>
     * This method allows you to provide a suite of {@link Option} to include in
     * the GET query. Options currently supported for GET are :
     * <ul>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a query that fetch the entity of PRIMARY KEY {@code objects}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement getQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultGetOptions);

        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option)o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return getQuery(pks, options);
    }

    private Statement getQuery(List<Object> primaryKeys, EnumMap<Option.Type, Option> options) {

        if (primaryKeys.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKeys.size()));

        BoundStatement bs = getPreparedQuery(QueryType.GET, options).bind();
        int i = 0;
        for (Object value : primaryKeys) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(i);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            }
            bs.setBytesUnsafe(i, column.getDataType().serialize(value, protocolVersion));
            i++;
        }

        for (Option opt : options.values()) {
            if (opt.isValidFor(QueryType.GET))
                opt.addToPreparedStatement(bs, i++);
        }

        if (mapper.readConsistency != null)
            bs.setConsistencyLevel(mapper.readConsistency);
        return bs;
    }

    /**
     * Fetch an entity based on its primary key.
     * <p>
     * This method is basically equivalent to: {@code map(getManager().getSession().execute(getQuery(objects))).one()}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return the entity fetched or {@code null} if it doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public T get(Object... objects) {
        return mapAliased(session().execute(getQuery(objects))).one();
    }

    /**
     * Fetch an entity based on its primary key asynchronously.
     * <p>
     * This method is basically equivalent to mapping the result of: {@code getManager().getSession().executeAsync(getQuery(objects))}.
     *
     * @param objects the primary key of the entity to fetch, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE query.
     * @return a future on the fetched entity. The return future will yield
     * {@code null} if said entity doesn't exist.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<T> getAsync(Object... objects) {
        return Futures.transform(session().executeAsync(getQuery(objects)), mapOneFunction);
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * <p>
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a query that delete {@code entity} (based on it's defined mapping) with
     * provided USING options.
     */
    public Statement deleteQuery(T entity, Option... options) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, toMapWithDefaults(options, defaultDeleteOptions));
    }

    /**
     * Creates a query that can be used to delete the provided entity.
     * <p>
     * This method is a shortcut that extract the PRIMARY KEY from the
     * provided entity and call {@link #deleteQuery(Object...)} with it.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     *
     * @param entity the entity to delete.
     * @return a query that delete {@code entity} (based on it's defined mapping).
     */
    public Statement deleteQuery(T entity) {
        List<Object> pks = new ArrayList<Object>();
        for (int i = 0; i < mapper.primaryKeySize(); i++) {
            pks.add(mapper.getPrimaryKeyColumn(i).getValue(entity));
        }

        return deleteQuery(pks, defaultDeleteOptions);
    }

    /**
     * Creates a query that can be used to delete an entity given its PRIMARY KEY.
     * <p>
     * The values provided must correspond to the columns composing the PRIMARY
     * KEY (in the order of said primary key). The values can also contain, after
     * specifying the primary keys columns, a suite of {@link Option} to include in
     * the DELETE query. Note : currently, only {@link com.datastax.driver.mapping.Mapper.Option.Timestamp}
     * is supported for DELETE queries.
     * <p>
     * This method is useful if you want to setup a number of options (tracing,
     * conistency level, ...) of the returned statement before executing it manually
     * or need access to the {@code ResultSet} object after execution (to get the
     * trace, the execution info, ...), but in other cases, calling {@link #delete}
     * or {@link #deleteAsync} is shorter.
     * This method allows you to provide a suite of {@link Option} to include in
     * the DELETE query. Options currently supported for DELETE are :
     * <ul>
     * <li>Timestamp</li>
     * <li>Consistency level</li>
     * <li>Tracing</li>
     * </ul>
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order of the primary key.
     *                Can be followed by {@link Option} to include in the DELETE
     *                query.
     * @return a query that delete the entity of PRIMARY KEY {@code primaryKey}.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public Statement deleteQuery(Object... objects) {
        // Order and duplicates matter for primary keys
        List<Object> pks = new ArrayList<Object>();
        EnumMap<Option.Type, Option> options = new EnumMap<Option.Type, Option>(defaultDeleteOptions);

        for (Object o : objects) {
            if (o instanceof Option) {
                Option option = (Option)o;
                options.put(option.type, option);
            } else {
                pks.add(o);
            }
        }
        return deleteQuery(pks, options);
    }

    private Statement deleteQuery(List<Object> primaryKey, EnumMap<Option.Type, Option> options) {
        if (primaryKey.size() != mapper.primaryKeySize())
            throw new IllegalArgumentException(String.format("Invalid number of PRIMARY KEY columns provided, %d expected but got %d", mapper.primaryKeySize(), primaryKey.size()));

        BoundStatement bs = getPreparedQuery(QueryType.DEL, options).bind();
        int i = 0;
        for (Option opt : options.values()) {
            if (opt.isValidFor(QueryType.DEL))
                opt.addToPreparedStatement(bs, i);
            if (opt.isIncludedInQuery())
                i++;
        }

        int columnNumber = 0;
        for (Object value : primaryKey) {
            ColumnMapper<T> column = mapper.getPrimaryKeyColumn(columnNumber);
            if (value == null) {
                throw new IllegalArgumentException(String.format("Invalid null value for PRIMARY KEY column %s (argument %d)", column.getColumnName(), i));
            }
            bs.setBytesUnsafe(i++, column.getDataType().serialize(value, protocolVersion));
            columnNumber++;
        }

        if (mapper.writeConsistency != null)
            bs.setConsistencyLevel(mapper.writeConsistency);
        return bs;
    }

    /**
     * Deletes an entity mapped by this mapper.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     */
    public void delete(T entity) {
        session().execute(deleteQuery(entity));
    }

    /**
     * Deletes an entity mapped by this mapper using provided options.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     */
    public void delete(T entity, Option... options) {
        session().execute(deleteQuery(entity, options));
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity))}.
     *
     * @param entity the entity to delete.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity) {
        return Futures.transform(session().executeAsync(deleteQuery(entity)), NOOP);
    }

    /**
     * Deletes an entity mapped by this mapper asynchronously using provided options.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(entity, options))}.
     *
     * @param entity  the entity to delete.
     * @param options the options to add to the DELETE query.
     * @return a future on the completion of the deletion.
     */
    public ListenableFuture<Void> deleteAsync(T entity, Option... options) {
        return Futures.transform(session().executeAsync(deleteQuery(entity, options)), NOOP);
    }

    /**
     * Deletes an entity based on its primary key.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().execute(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key.Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public void delete(Object... objects) {
        session().execute(deleteQuery(objects));
    }

    /**
     * Deletes an entity based on its primary key asynchronously.
     * <p>
     * This method is basically equivalent to: {@code getManager().getSession().executeAsync(deleteQuery(objects))}.
     *
     * @param objects the primary key of the entity to delete, or more precisely
     *                the values for the columns of said primary key in the order
     *                of the primary key. Can be followed by {@link Option} to include
     *                in the DELETE query.
     * @throws IllegalArgumentException if the number of value provided differ from
     *                                  the number of columns composing the PRIMARY KEY of the mapped class, or if
     *                                  at least one of those values is {@code null}.
     */
    public ListenableFuture<Void> deleteAsync(Object... objects) {
        return Futures.transform(session().executeAsync(deleteQuery(objects)), NOOP);
    }

    /**
     * Maps the rows from a {@code ResultSet} into the class this is a mapper of.
     * <p>
     * Use this method to map a result set that was not generated by the mapper (e.g. a result set coming from a
     * manual query or an {@link Accessor method}.
     * It expects that the result set contains all column mapped in the target class,
     * and that they are not aliased. {@link Computed} fields will not be filled in mapped objects.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     *
     * @see #mapAliased(ResultSet)
     */
    public Result<T> map(ResultSet resultSet) {
        return new Result<T>(resultSet, mapper, protocolVersion);
    }

    /**
     * Maps the rows from a {@code ResultSet} into the class this is a mapper of.
     * <p>
     * Use this method to map a result set coming from the execution of the {@code Statement} returned by
     * {@link #getQuery(Object...)}. {@link Computed} fields will be filled in the mapped object.
     *
     * @param resultSet the {@code ResultSet} to map.
     * @return the mapped result set. Note that the returned mapped result set
     * will encapsulate {@code resultSet} and so consuming results from this
     * returned mapped result set will consume results from {@code resultSet}
     * and vice-versa.
     *
     * @see #map(ResultSet)
     */
    public Result<T> mapAliased(ResultSet resultSet) {
        return (manager.supportsAliases)
            ? new Result<T>(resultSet, mapper, protocolVersion, true)
            : map(resultSet);
    }

    /**
     * Set the default save {@link Option} for this object mapper, that will be used
     * in all save operations unless overridden. Refer to {@link Mapper#save(Object, Option...)})}
     * to check available save options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultSaveOptions}.
     */
    public void setDefaultSaveOptions(Option... options) {
        this.defaultSaveOptions = toMap(options);
    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultSaveOptions() {
        this.defaultSaveOptions = NO_OPTIONS;
    }

    /**
     * Set the default get {@link Option} for this object mapper, that will be used
     * in all get operations unless overridden. Refer to {@link Mapper#get(Object...)} )} to check available
     * get options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultGetOptions}.
     */
    public void setDefaultGetOptions(Option... options) {
        this.defaultGetOptions = toMap(options);

    }

    /**
     * Reset the default save options for this object mapper.
     */
    public void resetDefaultGetOptions() {
        this.defaultGetOptions = NO_OPTIONS;
    }

    /**
     * Set the default delete {@link Option} for this object mapper, that will be used
     * in all delete operations unless overridden. Refer to {@link Mapper#delete(Object...)} )}
     * to check available delete options.
     *
     * @param options the options to set. To reset, use {@link Mapper#resetDefaultDeleteOptions}.
     */
    public void setDefaultDeleteOptions(Option... options) {
        this.defaultDeleteOptions = toMap(options);
    }

    /**
     * Reset the default delete options for this object mapper.
     */
    public void resetDefaultDeleteOptions() {
        this.defaultDeleteOptions = NO_OPTIONS;
    }

    private static EnumMap<Option.Type, Option> toMap(Option[] options) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(Option.Type.class);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    private static EnumMap<Option.Type, Option> toMapWithDefaults(Option[] options, EnumMap<Option.Type, Option> defaults) {
        EnumMap<Option.Type, Option> result = new EnumMap<Option.Type, Option>(defaults);
        for (Option option : options) {
            result.put(option.type, option);
        }
        return result;
    }

    /**
     * An option for a mapper operation.
     * <p>
     * Options can be passed to individual operations:
     * <pre>
     *     mapper.save(myObject, Option.ttl(3600));
     * </pre>
     *
     * The mapper can also have defaults, that will apply to all operations that do not
     * override these particular option:
     * <pre>
     *     mapper.setDefaultSaveOptions(Option.ttl(3600));
     *     mapper.save(myObject);
     * </pre>
     *
     * <p>
     * See the static methods in this class for available options.
     */
    public static abstract class Option {

        enum Type {TTL, TIMESTAMP, CL, TRACING}

        final Type type;

        protected Option(Type type) {
            this.type = type;
        }

        /**
         * Creates a new Option object to add time-to-live to a mapper operation. This is
         * only valid for save operations.
         *
         * @param ttl the TTL (in seconds).
         * @return the option.
         */
        public static Option ttl(int ttl) {
            return new Ttl(ttl);
        }

        /**
         * Creates a new Option object to add a timestamp to a mapper operation. This is
         * only valid for save and delete operations.
         *
         * @param timestamp the timestamp (in microseconds).
         * @return the option.
         */
        public static Option timestamp(long timestamp) {
            return new Timestamp(timestamp);
        }

        /**
         * Creates a new Option object to add a consistency level value to a mapper operation. This
         * is valid for save, delete and get operations.
         *
         * @param cl the {@link com.datastax.driver.core.ConsistencyLevel} to use for the operation.
         * @return the option.
         */
        public static Option consistencyLevel(ConsistencyLevel cl) {
            return new ConsistencyLevelOption(cl);
        }

        /**
         * Creates a new Option object to enable query tracing for a mapper operation.
         *
         * @param enabled whether to enable tracing.
         * @return the option.
         */
        public static Option tracing(boolean enabled) {
            return new Tracing(enabled);
        }

        abstract void appendTo(Insert.Options usings);

        abstract void appendTo(Delete.Options usings);

        abstract void addToPreparedStatement(BoundStatement bs, int i);

        abstract boolean isValidFor(QueryType qt);

        abstract boolean isIncludedInQuery();

        static class Ttl extends Option {

            private int ttlValue;

            Ttl(int value) {
                super(Type.TTL);
                this.ttlValue = value;
            }

            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.ttl(QueryBuilder.bindMarker()));
            }

            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setInt(i, this.ttlValue);
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE;
            }

            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class Timestamp extends Option {

            private long tsValue;

            Timestamp(long value) {
                super(Type.TIMESTAMP);
                this.tsValue = value;
            }

            void appendTo(Insert.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            void appendTo(Delete.Options usings) {
                usings.and(QueryBuilder.timestamp(QueryBuilder.bindMarker()));
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE || qt == QueryType.DEL;
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setLong(i, this.tsValue);
            }

            boolean isIncludedInQuery() {
                return true;
            }
        }

        static class ConsistencyLevelOption extends Option {

            private ConsistencyLevel cl;

            ConsistencyLevelOption(ConsistencyLevel cl) {
                super(Type.CL);
                this.cl = cl;
            }

            void appendTo(Insert.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                bs.setConsistencyLevel(cl);
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE
                    || qt == QueryType.DEL
                    || qt == QueryType.GET;
            }

            boolean isIncludedInQuery() {
                return false;
            }
        }

        static class Tracing extends Option {

            private boolean tracing;

            Tracing(boolean tracing) {
                super(Type.TRACING);
                this.tracing = tracing;
            }

            void appendTo(Insert.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            void appendTo(Delete.Options usings) {
                throw new UnsupportedOperationException("shouldn't be called");
            }

            void addToPreparedStatement(BoundStatement bs, int i) {
                if (this.tracing)
                    bs.enableTracing();
            }

            boolean isValidFor(QueryType qt) {
                return qt == QueryType.SAVE
                    || qt == QueryType.DEL
                    || qt == QueryType.GET;
            }

            boolean isIncludedInQuery() {
                return false;
            }
        }
    }

    private static class MapperQueryKey {
        private final QueryType queryType;
        private final EnumSet<Option.Type> optionTypes;

        MapperQueryKey(QueryType queryType, EnumMap<Option.Type, Option> options) {
            Preconditions.checkNotNull(queryType);
            Preconditions.checkNotNull(options);
            this.queryType = queryType;
            this.optionTypes = EnumSet.noneOf(Option.Type.class);
            for (Option opt : options.values()) {
                if (opt.isIncludedInQuery())
                    this.optionTypes.add(opt.type);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (other instanceof MapperQueryKey) {
                MapperQueryKey that = (MapperQueryKey)other;
                return this.queryType.equals(that.queryType)
                    && this.optionTypes.equals(that.optionTypes);
            }
            return false;
        }

        public int hashCode() {
            return Objects.hashCode(queryType, optionTypes);
        }
    }
}
