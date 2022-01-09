package com.oorlov.sandbox1;
import java.sql.*;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public interface IDataHandler {
    int getTotalRecordsAmount() throws SQLException;
    Row[] testSampleDataFetching(Dataset<Row> dataset) throws Exception;
    void saveToHdfs(Dataset<Row> dataset) throws Exception;
    SparkConf createSparkConfig();
    void startTransaction() throws Exception;
}
