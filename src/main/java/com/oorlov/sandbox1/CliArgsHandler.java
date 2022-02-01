package com.oorlov.sandbox1;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Logger;

public class CliArgsHandler implements ICliArgsHandler {
    private static final String DEFAULT_SPLIT_SYMBOL = "=";
    private final Logger logger;

    public CliArgsHandler(Logger logger) {
        if (logger == null)
            throw new NullArgumentException("The input logger object can't be null.");

        this.logger = logger;
    }

    public DtoArgsData createManagedDto(String[] cliRawArgs) throws CustomException {
        if (cliRawArgs == null || cliRawArgs.length == 0)
            throw new NullArgumentException("The given raw CLI-args object can't be null or equal zero.");

        logger.info(String.format("Provided CLI-arguments: %s", Arrays.toString(cliRawArgs)));
        ArrayList<String> args = new ArrayList<>(Arrays.asList(cliRawArgs));
        DtoArgsData dto = new DtoArgsData();

        for (String item : args) {
            String[] parts = item.split(DEFAULT_SPLIT_SYMBOL);
            String key     = parts[0];
            String value   = parts[1];

            switch (key) {
                case "jdbc_db_connstr":
                    dto.setJdbcConnectionString(value);
                    break;
                case "db_driver":
                    dto.setDbDriver(value);
                    break;
                case "db_user":
                    dto.setDbUser(value);
                    break;
                case "db_pwd":
                    dto.setDbPwd(value);
                    break;
                case "db_test_schema":
                    dto.setDbTestSchema(value);
                    break;
                case "db_import_table":
                    dto.setDbImportTable(value);
                    break;
                case "db_export_table":
                    dto.setDbExportTable(value);
                    break;
                case "db_count_alias":
                    dto.setDbCountAlias(value);
                    break;
                case "hdfs_host":
                    dto.setHdfsHost(value);
                    break;
                case "hdfs_input_path":
                    dto.setHdfsInputPath(value);
                    break;
                case "hdfs_output_path":
                    dto.setHdfsOutputPath(value);
                    break;
                case "slice_delta_value":
                    dto.setSliceDelta(Integer.parseInt(value));
                    break;
                case "core_pool_size":
                    dto.setCorePoolSize(Integer.parseInt(value));
                    break;
                case "maximum_pool_size":
                    dto.setMaximumPoolSize(Integer.parseInt(value));
                    break;
                case "keep_alive_time":
                    dto.setKeepAliveTime(Long.parseLong(value));
                    break;
                case "pool_queue_size":
                    dto.setPoolQueueSize(Integer.parseInt(value));
                    break;
                case "spark_master_host":
                    dto.setSparkMasterHost(value);
                    break;
                case "spark_app_name":
                    dto.setSparkAppName(value);
                    break;
                case "use_adb_connector":
                    dto.setAdbConnectorUsageValue(Boolean.parseBoolean(value));
                    break;
                case "tool_action":
                    dto.setToolAction(value);
                    break;
                default:
                    throw new CustomException(String.format("You've provided some invalid argument (k/v): %s/%s.", key, value));
            }
        }

        return dto;
    }
}
