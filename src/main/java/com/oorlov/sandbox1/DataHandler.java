package com.oorlov.sandbox1;
import java.sql.*;
import java.util.Properties;
import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

public class DataHandler implements IDataHandler {
    private final Logger _logger;
    private final IDtoArgsData _argsData;
    private final String DEFAULT_SAMPLE_FILTER_VALUE = "id < 5";

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

    private Dataset<Row> prepareDatasetProvider(SQLContext sqlContext) {
        if (sqlContext == null)
            throw new NullArgumentException("The input SQL-context object can't be null.");

        return (!_argsData.getAdbConnectorUsageValue())
        ? sqlContext.read().jdbc(_argsData.getJdbcConnectionString(), _argsData.getDbTestTable(), prepareDbProperties())
        : sqlContext.read()
        .format("adb")
        .option("spark.adb.url"      , _argsData.getJdbcConnectionString())
        .option("spark.adb.user"     , _argsData.getDbUser())
        .option("spark.adb.password" , _argsData.getDbPwd())
        .option("spark.adb.dbschema" , _argsData.getDbTestSchema())
        .option("spark.adb.dbtable"  , _argsData.getDbTestTable())
        .load();
    }

    public int getTotalRecordsAmount() throws SQLException {
        Connection connection = null;
        final String sqlQuery = String.format("SELECT COUNT(*) AS %s FROM %s.%s",
            _argsData.getDbCountAlias(),
            _argsData.getDbTestSchema(),
            _argsData.getDbTestTable()
        );

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

        _logger.info(String.format("There will be executed the next query: %s", sqlQuery));
        Statement statement = connection.createStatement();
        ResultSet results   = statement.executeQuery(sqlQuery);
        int totalCount      = -1;

        while (results.next()) {
            totalCount = results.getInt(_argsData.getDbCountAlias());
            _logger.info(String.format("COUNT(*)=%d", totalCount));
        }

        results.close();
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
            String value = String.format("id > %d and id < %d", (recordsPerCycle * i - recordsPerCycle), (recordsPerCycle * i));
            _logger.info(String.format("Cycle #%d , used filter value: %s", i, value));

            dataset.filter(value)
            .orderBy(functions.col("id").asc())
            .write()
            .parquet(String.format("%spart_%d", _argsData.getHdfsOutputPath(), i));
        }
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
        Dataset<Row> dataset          = prepareDatasetProvider(sqlContext);
        testSampleDataFetching(dataset);
        saveToHdfs(dataset);
        sparkContext.stop();
    }
}
