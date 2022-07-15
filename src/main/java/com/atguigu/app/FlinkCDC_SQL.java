package com.atguigu.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) {
        //TODO 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 2,使用DDL方式建表
        tableEnv.executeSql(
                "" +
                        "CREATE TABLE base_trademark ( " +
                        " id INT NOT NULL, " +
                        " tm_name STRING, " +
                        " logo_url STRING " +
                        ") WITH ( " +
                        " 'connector' = 'mysql-cdc', " +
                        " 'hostname' = 'hadoop102', " +
                        " 'port' = '3306', " +
                        " 'username' = 'root', " +
                        " 'password' = '123456', " +
                        " 'database-name' = 'gmall2022', " +
                        " 'table-name' = 'base_trademark', " +
                        " 'scan.incremental.snapshot.enabled' = 'false' " +
                        ")"
        );


        //TODO 3,查询并打印
        tableEnv.sqlQuery("select * from base_trademark")
                .execute()
                .print();





    }
}
