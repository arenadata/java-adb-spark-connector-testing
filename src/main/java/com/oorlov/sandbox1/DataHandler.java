package com.oorlov.sandbox1;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public class DataHandler implements IDataHandler {
    private final Logger       _logger;
    private final IDtoArgsData _argsData;
    private final String  DEFAULT_SAMPLE_FILTER_VALUE    = "id < 5";
    private final String  DEFAULT_HDFS_FS_CONFIG_KEY     = "fs.default.name";
    private final String  NOT_NEEDED_METAFILE_SUFFIX     = "_SUCCESS";
    private final Boolean DO_RECURSIVE_ITERATION_IN_HDFS = true;
    private final int     JDBC_RESULT_FOR_SAVED_ROW      = 1;
    private final int     DEFAULT_NAN_TOTAL_COUNT_VALUE  = -1;

    public DataHandler(Logger logger, IDtoArgsData argsData) {
        if (logger == null)
            throw new NullArgumentException("The input logger object can't be null.");

        if (argsData == null)
            throw new NullArgumentException("The input DTO CliArgs object can't be null.");

        _logger   = logger;
        _argsData = argsData;
    }

    private Properties prepareDbProperties() {
        Properties properties = new Properties();
        properties.put("user"     , _argsData.getDbUser());
        properties.put("password" , _argsData.getDbPwd());
        properties.put("driver"   , _argsData.getDbDriver());
        return properties;
    }

    private String getTargetTableName() throws Exception {
        String targetTable = null;
        EToolAction currentAction = _argsData.getToolAction();

        switch (currentAction) {
            case ReadRdbmsAndWriteToHdfs:
                targetTable = _argsData.getDbImportTable();
                break;
            case ReadHdfsAndWriteToRdbms:
                targetTable = _argsData.getDbExportTable();
                break;
            default:
                throw new Exception("Can't fetch the target table name, due the invalid current action value.");
        }

        _logger.info(String.format("The target table for '%s' action will be: '%s'.", currentAction, targetTable));
        return targetTable;
    }

    private Dataset<Row> prepareDatasetProvider(SQLContext sqlContext) throws Exception {
        if (sqlContext == null)
            throw new NullArgumentException("The input SQL-context object can't be null.");

        return (!_argsData.getAdbConnectorUsageValue())
        ? sqlContext.read().jdbc(_argsData.getJdbcConnectionString(), getTargetTableName(), prepareDbProperties())
        : sqlContext.read()
        .format("adb")
        .option("spark.adb.url"      , _argsData.getJdbcConnectionString())
        .option("spark.adb.user"     , _argsData.getDbUser())
        .option("spark.adb.password" , _argsData.getDbPwd())
        .option("spark.adb.dbschema" , _argsData.getDbTestSchema())
        .option("spark.adb.dbtable"  , getTargetTableName())
        .load();
    }

    private Connection prepareJdbcConnection() {
        Connection connection = null;

        try {
            Class.forName(_argsData.getDbDriver());
        } catch (ClassNotFoundException exception) {
            _logger.info("PostgreSQL JDBC Driver is not found. Include it in your library path.");
            exception.printStackTrace();
        }

        _logger.info("PostgreSQL JDBC Driver successfully connected.");

        try {
            connection = DriverManager.getConnection(
                _argsData.getJdbcConnectionString(),
                _argsData.getDbUser(),
                _argsData.getDbPwd()
            );
        } catch (SQLException exception) {
            _logger.error(String.format("Connection failed.\n%s", exception));
            exception.printStackTrace();
        }

        if (connection != null) {
            _logger.info("You've successfully connected to database, now.");
        } else {
            _logger.error("Failed to make connection to database.");
        }

        return connection;
    }

    private ArrayList<IManagedRowItem> prepareManagedRows(SQLContext sqlContext, ArrayList<Path> files) throws Exception {
        if (sqlContext == null)
            throw new NullArgumentException("The input SQL-context object can't be null.");

        if (files == null || files.size() == 0)
            throw new NullArgumentException("The input HDFS-path array can't be null or equal zero.");

        ArrayList<IManagedRowItem> managedItems = new ArrayList<>();

        for (Path hdfsPath : files) {
            _logger.info(String.format("Going to read next file from HDFS: %s", hdfsPath.toString()));
            Dataset<Row> dataset = sqlContext.read().parquet(hdfsPath.toString());
            Row row = dataset.first();
            _logger.info(String.format("Deserialized object from the parquet-file: %s", row.toString()));

            IManagedRowItem item = new ManagedRowItem();
            item.setId((Integer)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_ID));
            item.setDatetime((Date)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_DATETIME));
            item.setValue((String)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_VALUE));
            item.setPart((Integer)row.get(ManagedRowItem.PARQUET_POSITION_FIELD_PART));
            managedItems.add(item);
        }

        return managedItems;
    }

    public int getTotalRecordsAmount() throws Exception {
        final Connection connection = prepareJdbcConnection();
        final String targetTable    = getTargetTableName();
        final String sqlQuery       = String.format("SELECT COUNT(*) AS %s FROM %s.%s",
            _argsData.getDbCountAlias(),
            _argsData.getDbTestSchema(),
            targetTable
        );

        _logger.info(String.format("There will be executed the next query: %s", sqlQuery));
        Statement statement = connection.createStatement();
        ResultSet results   = statement.executeQuery(sqlQuery);
        int totalCount      = DEFAULT_NAN_TOTAL_COUNT_VALUE;

        while (results.next()) {
            totalCount = results.getInt(_argsData.getDbCountAlias());
            _logger.info(String.format("COUNT(*)=%d", totalCount));
        }

        results.close();
        connection.close();
        return totalCount;
    }

    public Row[] testSampleDataFetching(Dataset<Row> dataset) throws Exception {
        if (dataset == null)
            throw new Exception("The given dataset object can't be null.");

        Row[] data = (Row[])dataset.filter(DEFAULT_SAMPLE_FILTER_VALUE).collect();

        for (Row item : data)
            _logger.info(item);

        return data;
    }

    public void saveToHdfs(Dataset<Row> dataset) throws Exception {
        if (dataset == null)
            throw new Exception("The given dataset object can't be null.");

        int recordsAmount   = getTotalRecordsAmount();
        int recordsPerCycle = recordsAmount / _argsData.getSliceDelta();
        _logger.info(String.format("Total records amount: %d", recordsAmount));
        _logger.info(String.format("Records, which will be handled per single cycle: %d", recordsPerCycle));

        for (int i = 1; i <= _argsData.getSliceDelta(); i++) {
            String value = String.format("id >= %d and id < %d", (recordsPerCycle * i - recordsPerCycle), (recordsPerCycle * i));
            _logger.info(String.format("Cycle #%d , used filter value: %s", i, value));

            dataset.filter(value)
            .orderBy(functions.col("id").asc())
            .write()
            .parquet(String.format("%s%s_part_%d", _argsData.getHdfsHost(), _argsData.getHdfsOutputPath(), i));
        }
    }

    public ArrayList<IManagedRowItem> readFromHdfs(SQLContext sqlContext) throws Exception {
        if (sqlContext == null)
            throw new Exception("The given dataset object can't be null.");

        Configuration conf = new Configuration();
        conf.set(DEFAULT_HDFS_FS_CONFIG_KEY, _argsData.getHdfsHost());
        FileSystem fileSystem = FileSystem.get(conf);
        ArrayList<Path> files = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(_argsData.getHdfsInputPath()), DO_RECURSIVE_ITERATION_IN_HDFS);

        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            Path filePath = fileStatus.getPath();

            if (!filePath.getName().contains(NOT_NEEDED_METAFILE_SUFFIX))
                files.add(filePath);

            _logger.info(filePath);
        }

        return prepareManagedRows(sqlContext, files);
    }

    public void saveToRdbms(ArrayList<IManagedRowItem> inputData) throws Exception {
        final Connection connection = prepareJdbcConnection();
        final String queryTemplate  = String.format(
         "INSERT INTO %s.%s VALUES(?, ?, ?, ?)",
            _argsData.getDbTestSchema(),
            getTargetTableName()
        );

        for (IManagedRowItem item : inputData) {
            PreparedStatement statement = connection.prepareStatement(queryTemplate);
            statement.setInt(ManagedRowItem.STATEMENT_POSITION_FIELD_ID, item.getId());
            statement.setDate(ManagedRowItem.STATEMENT_POSITION_FIELD_DATETIME, item.getDatetime());
            statement.setString(ManagedRowItem.STATEMENT_POSITION_FIELD_VALUE, item.getValue());
            statement.setInt(ManagedRowItem.STATEMENT_POSITION_FIELD_PART, item.getPart());

            _logger.info(String.format("There will be executed the next query: %s", statement));
            int result = statement.executeUpdate();

            if (result != JDBC_RESULT_FOR_SAVED_ROW)
                throw new Exception("The row wasn't saved to the PostgreSQL/Greenplum correctly.");
        }

        connection.close();
    }

    public SparkConf createSparkConfig() {
        return new SparkConf()
        .setAppName(_argsData.getSparkAppName())
        .setMaster(_argsData.getSparkMasterHost());
    }

    public void startTransaction() throws Exception {
        SparkConf sparkConf           = createSparkConfig();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext         = new SQLContext(sparkContext);

        switch (_argsData.getToolAction()) {
            case ReadRdbmsAndWriteToHdfs:
                Dataset<Row> dataset = prepareDatasetProvider(sqlContext);
                saveToHdfs(dataset);
                break;
            case ReadHdfsAndWriteToRdbms:
                ArrayList<IManagedRowItem> items = readFromHdfs(sqlContext);
                saveToRdbms(items);
                break;
            default:
                throw new Exception("There is no supported tool action you've provided.");
        }

        sparkContext.stop();
    }
}
