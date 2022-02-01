package com.oorlov.sandbox1;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public interface IDataHandler {
    static void checkInputString(String value) {
        if (value == null || value.length() == 0)
            throw new NullArgumentException("The given string object can't be null or equal zero.");
    }

    int getTotalRecordsAmount() throws CustomException, SQLException;
    void saveToHdfs(Dataset<Row> dataset) throws CustomException, SQLException;
    ArrayList<IManagedRowItem> handleSavedHdfsData(SparkSession sparkSession) throws IOException;
    void saveToRdbms(ArrayList<IManagedRowItem> inputData) throws CustomException, SQLException;
    SparkConf createSparkConfig();
    void startTransaction();
    ArrayList<IManagedRowItem> prepareManagedRows(SparkSession sparkSession, ArrayList<Path> files);
}
