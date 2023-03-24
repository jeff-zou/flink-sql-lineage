package com.hw.flink.lineage;


import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.ddl.CreateTableASOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverterFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.MiniBatchInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @description: LineageContext
 * @author: HamaWhite
 * @version: 1.0.0
 * @date: 2022/8/6 11:06 AM
 */
public class LineageContext {
    private static final Logger LOG = LoggerFactory.getLogger(LineageContext.class);
    private static final String DELIMITER = ".";
    private final TableEnvironmentImpl tableEnv;
    private final FlinkChainedProgram<StreamOptimizeContext> flinkChainedProgram;

    public LineageContext(AbstractCatalog catalog) {
        Configuration configuration = new Configuration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        this.tableEnv = (TableEnvironmentImpl) StreamTableEnvironment.create(env, settings);

        tableEnv.registerCatalog(catalog.getName(), catalog);
        tableEnv.useCatalog(catalog.getName());

        this.flinkChainedProgram = FlinkStreamProgramWithoutPhysical.buildProgram(configuration);
    }


    /**
     * Execute the single sql
     */
    public TableResult execute(String singleSql) {
        return tableEnv.executeSql(singleSql);
    }

    /**
     * Parse the field blood relationship of the input SQL
     */
    public List<Result> parseFieldLineage(String sql) {
        LOG.info("Input Sql: \n {}", sql);
        // 1. Generate original relNode tree
        Tuple2<String, RelNode> parsed = parseStatement(sql);
        String sinkTable = parsed.getField(0);
        RelNode oriRelNode = parsed.getField(1);

        // 2. Optimize original relNode to generate Optimized Logical Plan
        RelNode optRelNode = optimize(oriRelNode);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Original RelNode: \n {}", oriRelNode.explain());
            LOG.debug("Optimized RelNode: \n {}", optRelNode.explain());
        }

        // 3. Build lineage based from RelMetadataQuery
        return buildFiledLineageResult(sinkTable, optRelNode);
    }


    private Tuple2<String, RelNode> parseStatement(String sql) {
        List<Operation> operations = tableEnv.getParser().parse(sql);

        if (operations.size() != 1) {
            throw new TableException(
                    "Unsupported SQL query! only accepts a single SQL statement.");
        }
        Operation operation = operations.get(0);
        // process create table as select
        operation = prePocessCprereateTableASOperation(operation);

        if (operation instanceof SinkModifyOperation) {
            SinkModifyOperation sinkOperation = (SinkModifyOperation) operation;

            PlannerQueryOperation queryOperation = (PlannerQueryOperation) sinkOperation.getChild();
            RelNode relNode = queryOperation.getCalciteTree();
            return new Tuple2<>(
                    sinkOperation.getContextResolvedTable().getIdentifier().asSummaryString(),
                    relNode);
        } else {
            throw new TableException("Only insert is supported now.");
        }
    }

    /**
     * There are two parts in CTAS, the SELECT part can be any SELECT query supported by Flink SQL.
     * The CREATE part takes the resulting schema from the SELECT part and creates the target table.
     *
     * This method creates a new table and returns the remaining insert for subsequent lineage.
     */
    private Operation prePocessCprereateTableASOperation(Operation operation) {
        if (operation instanceof CreateTableASOperation) {
            CreateTableASOperation ctasOperation = (CreateTableASOperation) operation;
            CreateTableOperation createTableOperation = ctasOperation.getCreateTableOperation();

            if (createTableOperation.isTemporary()) {
                tableEnv.getCatalogManager().createTemporaryTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            } else {
                tableEnv.getCatalogManager().createTable(
                        createTableOperation.getCatalogTable(),
                        createTableOperation.getTableIdentifier(),
                        createTableOperation.isIgnoreIfExists());
            }
            return ctasOperation.toSinkModifyOperation(tableEnv.getCatalogManager());
        }
        return operation;
    }

    /**
     * Calling each program's optimize method in sequence.
     */
    private RelNode optimize(RelNode relNode) {
        return flinkChainedProgram.optimize(relNode, new StreamOptimizeContext() {
            @Override
            public ClassLoader getClassLoader() {
                return getPlanner().getFlinkContext().getClassLoader();
            }

            @Override
            public ModuleManager getModuleManager() {
                return getPlanner().moduleManager();
            }

            @Override
            public boolean isBatchMode() {
                return false;
            }

            @Override
            public TableConfig getTableConfig() {
                return tableEnv.getConfig();
            }

            @Override
            public FunctionCatalog getFunctionCatalog() {
                return getPlanner().getFlinkContext().getFunctionCatalog();
            }

            @Override
            public CatalogManager getCatalogManager() {
                return tableEnv.getCatalogManager();
            }

            @Override
            public SqlExprToRexConverterFactory getSqlExprToRexConverterFactory() {
                return getPlanner().getFlinkContext().getSqlExprToRexConverterFactory();
            }

            @Override
            public <C> C unwrap(Class<C> clazz) {
                return getPlanner().getFlinkContext().unwrap(clazz);

            }

            @Override
            public FlinkRelBuilder getFlinkRelBuilder() {
                return getPlanner().getRelBuilder();
            }

            @Override
            public boolean needFinalTimeIndicatorConversion() {
                return true;
            }

            @Override
            public boolean isUpdateBeforeRequired() {
                return false;
            }

            @Override
            public MiniBatchInterval getMiniBatchInterval() {
                return MiniBatchInterval.NONE;
            }


            @SuppressWarnings("squid:S5803")
            private PlannerBase getPlanner() {
                return (PlannerBase) tableEnv.getPlanner();
            }
        });
    }

    private List<Result> buildFiledLineageResult(String sinkTable, RelNode optRelNode) {
        // target columns
        List<String> targetColumnList = tableEnv.from(sinkTable)
                .getResolvedSchema()
                .getColumnNames();

        // check the size of query and sink fields match
        validateSchema(sinkTable, optRelNode, targetColumnList);

        RelMetadataQuery metadataQuery = optRelNode.getCluster().getMetadataQuery();
        List<Result> resultList = new ArrayList<>();

        for (int index = 0; index < targetColumnList.size(); index++) {
            String targetColumn = targetColumnList.get(index);

            LOG.debug("**********************************************************");
            LOG.debug("Target table: {}", sinkTable);
            LOG.debug("Target column: {}", targetColumn);

            Set<RelColumnOrigin> relColumnOriginSet = metadataQuery.getColumnOrigins(optRelNode, index);

            if (CollectionUtils.isNotEmpty(relColumnOriginSet)) {
                for (RelColumnOrigin rco : relColumnOriginSet) {
                    // table
                    RelOptTable table = rco.getOriginTable();
                    String sourceTable = String.join(DELIMITER, table.getQualifiedName());

                    // filed
                    int ordinal = rco.getOriginColumnOrdinal();
                    List<String> fieldNames = ((TableSourceTable) table).contextResolvedTable().getResolvedSchema().getColumnNames();
                    String sourceColumn = fieldNames.get(ordinal);
                    LOG.debug("----------------------------------------------------------");
                    LOG.debug("Source table: {}", sourceTable);
                    LOG.debug("Source column: {}", sourceColumn);
                    if (StringUtils.isNotEmpty(rco.getTransform())) {
                        LOG.debug("transform: {}", rco.getTransform());
                    }
                    // add record
                    resultList.add(buildResult(sourceTable, sourceColumn, sinkTable, targetColumn, rco.getTransform()));
                }
            }
        }
        return resultList;
    }


    /**
     * Check the size of query and sink fields match
     */
    private void validateSchema(String sinkTable, RelNode relNode, List<String> sinkFieldList) {
        List<String> queryFieldList = relNode.getRowType().getFieldNames();
        if (queryFieldList.size() != sinkFieldList.size()) {
            throw new ValidationException(
                    String.format(
                            "Column types of query result and sink for %s do not match.\n"
                                    + "Query schema: %s\n"
                                    + "Sink schema:  %s",
                            sinkTable, queryFieldList, sinkFieldList));
        }
    }


    private Result buildResult(String sourceTablePath, String sourceColumn
            , String targetTablePath, String targetColumn, String transform) {
        String[] sourceItems = sourceTablePath.split("\\" + DELIMITER);
        String[] targetItems = targetTablePath.split("\\" + DELIMITER);

        return Result.builder()
                .sourceCatalog(sourceItems[0])
                .sourceDatabase(sourceItems[1])
                .sourceTable(sourceItems[2])
                .sourceColumn(sourceColumn)
                .targetCatalog(targetItems[0])
                .targetDatabase(targetItems[1])
                .targetTable(targetItems[2])
                .targetColumn(targetColumn)
                .transform(transform)
                .build();
    }
}
