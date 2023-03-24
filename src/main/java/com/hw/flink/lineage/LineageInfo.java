package com.hw.flink.lineage;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.scanner.Constant;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author: Jeff Zou @Date: 2023/3/22 14:47
 */
@Data
@Builder
@AllArgsConstructor
public class LineageInfo {
    public static final String DELIMITER = ".";

    private String sourceCatalog;

    private String sourceDatabase;

    private String sourceTable;

    private String sourceColumn;

    private String targetCatalog;

    private String targetDatabase;

    private String targetTable;

    private String targetColumn;

    /**
     * Stores the expression for data conversion,
     * which source table fields are transformed by which expression the target field
     */
    private String transform;

    public LineageInfo(String sourceTablePath, String sourceColumn
            , String targetTablePath, String targetColumn, String transform) {
        String[] sourceItems = sourceTablePath.split("\\" + DELIMITER);
        String[] targetItems = targetTablePath.split("\\" + DELIMITER);

        this.sourceCatalog = sourceItems[0];
        this.sourceDatabase = sourceItems[1];
        this.sourceTable = sourceItems[2];
        this.sourceColumn = sourceColumn;
        this.targetCatalog = targetItems[0];
        this.targetDatabase = targetItems[1];
        this.targetTable = targetItems[2];
        this.targetColumn = targetColumn;
        this.transform = transform;
    }

    public LineageInfo(String catalog, String database, String sourceTable, String sourceColumn
            , String targetTable, String targetColumn) {
        this.sourceCatalog = catalog;
        this.sourceDatabase = database;
        this.sourceTable = sourceTable;
        this.sourceColumn = sourceColumn;
        this.targetCatalog = catalog;
        this.targetDatabase = database;
        this.targetTable = targetTable;
        this.targetColumn = targetColumn;
    }

    public static List<LineageInfo> buildResult(String catalog, String database, String[][] expectedArray) {
        return Stream.of(expectedArray)
                .map(e -> {
                    LineageInfo result = new LineageInfo(catalog, database, e[0], e[1], e[2], e[3]);
                    // transform field is optional
                    if (e.length == 5) {
                        result.setTransform(e[4]);
                    }
                    return result;
                }).collect(Collectors.toList());
    }
}
