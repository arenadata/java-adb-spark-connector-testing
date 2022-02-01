package com.oorlov.sandbox1;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class ParallelRowsHandler extends Thread {
    private final Logger logger;
    private final DataHandler dataHandler;
    private final SparkSession sparkSession;
    private ParquetCollection collection;
    private ArrayList<IManagedRowItem> managedRows;

    public ParallelRowsHandler(Logger logger, DataHandler dataHandler, SparkSession sparkSession, Object parameter) {
        this.logger       = logger;
        this.dataHandler  = dataHandler;
        this.sparkSession = sparkSession;
        this.collection   = (ParquetCollection)parameter;
    }

    public void run() {
        logger.info("Will handle the parquet-parts in a new thread.");
        managedRows = dataHandler.prepareManagedRows(sparkSession, this.collection.files);

        try {
            dataHandler.saveToRdbms(managedRows);
        } catch (CustomException exception) {
            logger.error(exception);
        } catch (SQLException exception) {
            logger.error(exception);
        }
    }
}
