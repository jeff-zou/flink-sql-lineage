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


    @Test
    public void analyzeLineageJoin() {
        LineageServiceImpl lineageService = new LineageServiceImpl();
        lineageService.execute("create table sink_table (username string, level string) with ('connector'='print')");
        String source =
                "create table source_table(username varchar, level varchar, proctime as procTime()) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";

        String join =
                "create table join_table(username varchar, level varchar) "
                        + "with ('connector'='datagen',  'rows-per-second'='1', "
                        + "'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='9',"
                        + "'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='9'"
                        + ")";
        lineageService.execute(source);
        lineageService.execute(join);
        List<LineageResult> list = lineageService.analyzeLineage("insert into sink_table select s.username,j.level from source_table s " +
                "left join join_table for system_time as of s.proctime as j on j.username = s.username  ");
        System.out.println();
    }
}
