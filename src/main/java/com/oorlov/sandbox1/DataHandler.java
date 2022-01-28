package com.oorlov.sandbox1;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public class DataHandler implements IDataHandler {
    private static final String  DEFAULT_HDFS_FS_CONFIG_KEY     = "fs.default.name";
    private static final String  NOT_NEEDED_METAFILE_SUFFIX     = "_SUCCESS";
    private static final Boolean DO_RECURSIVE_ITERATION_IN_HDFS = true;
    private static final int     JDBC_RESULT_FOR_SAVED_ROW      = 1;
    private static final int     DEFAULT_NAN_TOTAL_COUNT_VALUE  = -1;
    private final Logger       logger;
    private final IDtoArgsData argsData;

    public DataHandler(Logger logger, IDtoArgsData argsData) {
        if (logger == null)
            throw new NullArgumentException("The input logger object can't be null.");

        if (argsData == null)
            throw new NullArgumentException("The input DTO CliArgs object can't be null.");

        this.logger   = logger;
        this.argsData = argsData;
    }

    private Properties prepareDbProperties() {
        Properties properties = new Properties();
        properties.put("user"     , argsData.getDbUser());
        properties.put("password" , argsData.getDbPwd());
        properties.put("driver"   , argsData.getDbDriver());
        return properties;
    }

    private String getTargetTableName() throws CustomException {
        String targetTable;
        EToolAction currentAction = argsData.getToolAction();

        switch (currentAction) {
            case READ_RDBMS_AND_WRITE_TO_HDFS:
                targetTable = argsData.getDbImportTable();
                break;
            case READ_HDFS_AND_WRITE_TO_RDBMS:
                targetTable = argsData.getDbExportTable();
                break;
            default:
                throw new CustomException("Can't fetch the target table name, due the invalid current action value.");
        }

        logger.info(String.format("The target table for '%s' action will be: '%s'.", currentAction, targetTable));
        return targetTable;
    }

    private Dataset<Row> prepareDatasetObject(SparkSession sparkSession) {
        if (sparkSession == null)
            throw new NullArgumentException("Can't prepare the dataset object, due the input Spark-session object can't be null.");

        Dataset<Row> dataset = null;

        try {
            if (!argsData.getAdbConnectorUsageValue()) {
                dataset = sparkSession.read().jdbc(argsData.getJdbcConnectionString(), getTargetTableName(), prepareDbProperties());
            } else {
                dataset = sparkSession.read()
                          .format("adb")
                          .option("spark.adb.url", argsData.getJdbcConnectionString())
                          .option("spark.adb.user", argsData.getDbUser())
                          .option("spark.adb.password", argsData.getDbPwd())
                          .option("spark.adb.dbschema", argsData.getDbTestSchema())
                          .option("spark.adb.dbtable", getTargetTableName())
                          .load();
            }
        } catch (Exception exception) {
            logger.error(exception);
        }

        return dataset;
    }

    private Connection prepareJdbcConnection() {
        Connection connection = null;

        try {
            Class.forName(argsData.getDbDriver());
        } catch (ClassNotFoundException exception) {
            logger.info("PostgreSQL JDBC Driver is not found. Include it in your library path.");
        }

        logger.info("PostgreSQL JDBC Driver successfully connected.");

        try {
            connection = DriverManager.getConnection(
                argsData.getJdbcConnectionString(),
                argsData.getDbUser(),
                argsData.getDbPwd()
            );
        } catch (SQLException exception) {
            logger.error(exception);
        }

        if (connection != null) {
            logger.info("You've successfully connected to database, now.");
        } else {
            logger.error("Failed to make connection to database.");
        }

        return connection;
    }

    private ArrayList<IManagedRowItem> prepareManagedRows(SparkSession sparkSession, ArrayList<Path> files) {
        if (sparkSession == null)
            throw new NullArgumentException("Can't prepare the managed rows 'arraylist', due the input Spark-session object can't be null.");

        if (files == null || files.isEmpty())
            throw new NullArgumentException("The input HDFS-path array can't be null or its size can't be equal zero.");

        ArrayList<IManagedRowItem> managedItems = new ArrayList<>();

        for (Path hdfsPath : files) {
            logger.info(String.format("Going to read next file from HDFS: %s", hdfsPath.toString()));
            Dataset<Row> dataset = sparkSession.read().parquet(hdfsPath.toString());
            Row row = dataset.first();
            logger.info(String.format("Deserialized object from the parquet-file: %s", row.toString()));

            IManagedRowItem item = new ManagedRowItem();
            item.setId((Integer)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_ID));
            item.setDatetime((Date)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_DATETIME));
            item.setValue((String)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_VALUE));
            item.setPart((Integer)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_PART));
            managedItems.add(item);
        }

        return managedItems;
    }

    public int getTotalRecordsAmount() throws CustomException, SQLException {
        int totalCount              = DEFAULT_NAN_TOTAL_COUNT_VALUE;
        final Connection connection = prepareJdbcConnection();
        final String targetTable    = getTargetTableName();
        final String sqlQuery       = String.format("SELECT COUNT(*) AS %s FROM %s.%s",
            argsData.getDbCountAlias(),
            argsData.getDbTestSchema(),
            targetTable
        );

        if (connection == null)
            throw new NullPointerException("The given JDBC-connection object is null.");

        logger.info(String.format("There will be executed the next query: %s", sqlQuery));
        Statement statement = null;

        try {
            statement = connection.createStatement();
            final ResultSet results = statement.executeQuery(sqlQuery);

            while (results.next()) {
                totalCount = results.getInt(argsData.getDbCountAlias());
                logger.info(String.format("COUNT(*)=%d", totalCount));
            }

            results.close();
        } catch (Exception exception) {
            logger.error(exception);
        } finally {
            if (statement != null)
                statement.close();

            connection.close();
        }

        return totalCount;
    }

    public void saveToHdfs(Dataset<Row> dataset) throws CustomException, SQLException {
        if (dataset == null)
            throw new NullArgumentException("The given dataset object can't be null.");

        int recordsAmount   = getTotalRecordsAmount();
        int recordsPerCycle = recordsAmount / argsData.getSliceDelta();
        logger.info(String.format("Total records amount: %d", recordsAmount));
        logger.info(String.format("Records, which will be handled per single cycle: %d", recordsPerCycle));

        for (int i = 1; i <= argsData.getSliceDelta(); i++) {
            String value = String.format("id >= %d and id < %d", (recordsPerCycle * i - recordsPerCycle), (recordsPerCycle * i));
            logger.info(String.format("Cycle #%d , used filter value: %s", i, value));

            dataset.filter(value)
            .orderBy(functions.col("id").asc())
            .write()
            .parquet(String.format("%s%s_part_%d", argsData.getHdfsHost(), argsData.getHdfsOutputPath(), i));
        }
    }

    public ArrayList<IManagedRowItem> readFromHdfs(SparkSession sparkSession) throws IOException {
        if (sparkSession == null)
            throw new NullArgumentException("Can't read from HDFS, due the input Spark-session object can't be null.");

        Configuration conf = new Configuration();
        conf.set(DEFAULT_HDFS_FS_CONFIG_KEY, argsData.getHdfsHost());
        FileSystem fileSystem = FileSystem.get(conf);
        ArrayList<Path> files = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(argsData.getHdfsInputPath()), DO_RECURSIVE_ITERATION_IN_HDFS);

        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            Path filePath = fileStatus.getPath();

            if (!filePath.getName().contains(NOT_NEEDED_METAFILE_SUFFIX))
                files.add(filePath);

            logger.info(filePath);
        }

        return prepareManagedRows(sparkSession, files);
    }

    public void saveToRdbms(ArrayList<IManagedRowItem> inputData) throws CustomException, SQLException {
        if (inputData == null || inputData.isEmpty())
            throw new NullArgumentException("The given input data collection can't be null or its size can't be equal zero.");

        PreparedStatement statement = null;
        final Connection connection = prepareJdbcConnection();
        final String queryTemplate  = String.format(
         "INSERT INTO %s.%s VALUES(?, ?, ?, ?)",
            argsData.getDbTestSchema(),
            getTargetTableName()
        );

        if (connection == null)
            throw new NullPointerException("The given JDBC-connection object is null.");

        try {
            for (IManagedRowItem item : inputData) {
                statement = connection.prepareStatement(queryTemplate);
                statement.setInt(ManagedRowItem.STATEMENT_POSITION_FIELD_ID, item.getId());
                statement.setDate(ManagedRowItem.STATEMENT_POSITION_FIELD_DATETIME, item.getDatetime());
                statement.setString(ManagedRowItem.STATEMENT_POSITION_FIELD_VALUE, item.getValue());
                statement.setInt(ManagedRowItem.STATEMENT_POSITION_FIELD_PART, item.getPart());

                logger.info(String.format("There will be executed the next query: %s", statement));
                int result = statement.executeUpdate();

                if (result != JDBC_RESULT_FOR_SAVED_ROW)
                    throw new CustomException("The row wasn't saved to the PostgreSQL/Greenplum correctly.");
            }
        } catch (Exception exception) {
            logger.error(exception);
        } finally {
            if (statement != null)
                statement.close();

            connection.close();
        }
    }

    public SparkConf createSparkConfig() {
        return new SparkConf()
        .setAppName(argsData.getSparkAppName())
        .setMaster(argsData.getSparkMasterHost());
    }

    public void startTransaction() {
        SparkSession sparkSession = null;

        try {
            final SparkSession.Builder builder = SparkSession.builder();
            SparkConf sparkConfiguration = createSparkConfig();
            sparkSession = builder.config(sparkConfiguration).getOrCreate();

            switch (argsData.getToolAction()) {
                case READ_RDBMS_AND_WRITE_TO_HDFS:
                    final Dataset<Row> dataset = prepareDatasetObject(sparkSession);
                    saveToHdfs(dataset);
                    break;
                case READ_HDFS_AND_WRITE_TO_RDBMS:
                    final ArrayList<IManagedRowItem> items = readFromHdfs(sparkSession);
                    saveToRdbms(items);
                    break;
                default:
                    throw new CustomException("There is no supported tool action you've provided.");
            }
        } catch (Exception exception) {
            logger.error(exception);
        } finally {
            if (sparkSession != null)
                sparkSession.close();
        }
    }
}
