package com.oorlov.sandbox1;

public class DtoArgsData implements IDtoArgsData {
    private static final int DEFAULT_MULTIPLE_VALUE = 100;
    private String  jdbcConnectionString;
    private String  dbDriver;
    private String  dbUser;
    private String  dbPwd;
    private String  dbTestSchema;
    private String  dbImportTable;
    private String  dbExportTable;
    private String  dbCountAlias;
    private String  hdfsHost;
    private String  hdfsInputPath;
    private String  hdfsOutputPath;
    private String  sparkMasterHost;
    private String  sparkAppName;
    private boolean useAdbConnector = false;
    private int     sliceDelta      = -1;
    private EToolAction toolAction  = EToolAction.NONE;

    public String getJdbcConnectionString() {
        return this.jdbcConnectionString;
    }

    public void setJdbcConnectionString(String value) {
        IDataHandler.checkInputString(value);
        this.jdbcConnectionString = value;
    }

    public String getDbDriver() {
        return this.dbDriver;
    }

    public void setDbDriver(String value) {
        IDataHandler.checkInputString(value);
        this.dbDriver = value;
    }

    public String getDbUser() {
        return this.dbUser;
    }

    public void setDbUser(String value) {
        IDataHandler.checkInputString(value);
        this.dbUser = value;
    }

    public String getDbPwd() {
        return this.dbPwd;
    }

    public void setDbPwd(String value) {
        IDataHandler.checkInputString(value);
        this.dbPwd = value;
    }

    public String getDbTestSchema() {
        return this.dbTestSchema;
    }

    public void setDbTestSchema(String value) {
        IDataHandler.checkInputString(value);
        this.dbTestSchema = value;
    }

    public String getDbImportTable() {
        return this.dbImportTable;
    }

    public void setDbImportTable(String value) {
        IDataHandler.checkInputString(value);
        this.dbImportTable = value;
    }

    public String getDbExportTable() {
        return this.dbExportTable;
    }

    public void setDbExportTable(String value) {
        IDataHandler.checkInputString(value);
        this.dbExportTable = value;
    }

    public String getDbCountAlias() {
        return this.dbCountAlias;
    }

    public void setDbCountAlias(String value) {
        IDataHandler.checkInputString(value);
        this.dbCountAlias = value;
    }

    public String getHdfsHost() {
        return this.hdfsHost;
    }

    public void setHdfsHost(String value) {
        IDataHandler.checkInputString(value);
        this.hdfsHost = value;
    }

    public String getHdfsInputPath() {
        return this.hdfsInputPath;
    }

    public void setHdfsInputPath(String value) {
        IDataHandler.checkInputString(value);
        this.hdfsInputPath = value;
    }

    public String getHdfsOutputPath() {
        return this.hdfsOutputPath;
    }

    public void setHdfsOutputPath(String value) {
        IDataHandler.checkInputString(value);
        this.hdfsOutputPath = value;
    }

    public String getSparkMasterHost() {
        return this.sparkMasterHost;
    }

    public void setSparkMasterHost(String value) {
        IDataHandler.checkInputString(value);
        this.sparkMasterHost = value;
    }

    public String getSparkAppName() {
        return this.sparkAppName;
    }

    public void setSparkAppName(String value) {
        IDataHandler.checkInputString(value);
        this.sparkAppName = value;
    }

    public void setAdbConnectorUsageValue(boolean value) { this.useAdbConnector = value; }

    public boolean getAdbConnectorUsageValue() { return this.useAdbConnector; }

    public int getSliceDelta() {
        return this.sliceDelta;
    }

    public void setSliceDelta(int value) throws CustomException {
        if (value % DEFAULT_MULTIPLE_VALUE != 0)
            throw new CustomException(String.format("The input integer value isn't multiple of %s.", DEFAULT_MULTIPLE_VALUE));

        this.sliceDelta = value;
    }

    public EToolAction getToolAction() { return this.toolAction; }

    public void setToolAction(String value) throws CustomException {
        IDataHandler.checkInputString(value);

        switch (value) {
            case "fromhdfstordbms":
                this.toolAction = EToolAction.READ_HDFS_AND_WRITE_TO_RDBMS;
                break;
            case "fromrdbmstohdfs":
                this.toolAction = EToolAction.READ_RDBMS_AND_WRITE_TO_HDFS;
                break;
            default:
                throw new CustomException("Invalid value for the 'tool action'. Possible values are: fromrdbmstohdfs, fromhdfstordbms");
        }
    }
}
