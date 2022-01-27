package com.oorlov.sandbox1;
import java.sql.*;
import java.util.ArrayList;
import org.apache.commons.lang.NullArgumentException;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public interface IDataHandler {
    static void checkInputString(String value) throws Exception {
        if (value == null)
            throw new NullArgumentException("The given string object can't be null");

        if (value.length() == 0)
            throw new Exception("The given string value mustn't be equal zero.");
    }

    int getTotalRecordsAmount() throws Exception;
    Row[] testSampleDataFetching(Dataset<Row> dataset) throws Exception;
    void saveToHdfs(Dataset<Row> dataset) throws Exception;
    SparkConf createSparkConfig();
    void startTransaction() throws Exception;
    ArrayList<IManagedRowItem> readFromHdfs(SQLContext sqlContext) throws Exception;
    void saveToRdbms(ArrayList<IManagedRowItem> inputData) throws Exception;
}
