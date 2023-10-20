package com.hw.lineage.flink;

import com.hw.lineage.common.model.LineageResult;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class LineageServiceImplTest {

    @Test
    public void analyzeLineage() {
        LineageServiceImpl lineageService = new LineageServiceImpl();
        lineageService.execute("create table b (a string) with ('connector'='print')");
        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";
        lineageService.execute(source);
        List<LineageResult> list = lineageService.analyzeLineage("insert into b select username from source_table");
        System.out.println();
    }
}
