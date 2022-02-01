package com.oorlov.sandbox1;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class ParallelRowsHandler implements Runnable {
    private final Logger logger;
    private final DataHandler dataHandler;
    private final SparkSession sparkSession;
    private ParquetCollection collection;

    public ParallelRowsHandler(Logger logger, DataHandler dataHandler, SparkSession sparkSession, Object parameter) {
        if (logger == null)
            throw new NullArgumentException("The input logger object can't be null.");

        if (dataHandler == null)
            throw new NullArgumentException("The input DTO CliArgs object can't be null.");

        if (sparkSession == null)
            throw new NullArgumentException("The input Spark-session object can't be null.");

        if (parameter == null)
            throw new NullArgumentException("The input boxed thread parameter can't be null.");

        this.logger       = logger;
        this.dataHandler  = dataHandler;
        this.sparkSession = sparkSession;
        this.collection   = (ParquetCollection)parameter;
    }

    @Override
    public void run() {
        logger.info("Will handle the parquet-parts in a new thread.");
        ArrayList<IManagedRowItem> managedRows = dataHandler.prepareManagedRows(sparkSession, (ArrayList<Path>)this.collection.getFiles());

        try {
            dataHandler.saveToRdbms(managedRows);
        } catch (CustomException | SQLException exception) {
            logger.error(exception);
        }
    }
}
