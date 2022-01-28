package com.oorlov.sandbox1;

public interface IDtoArgsData {
    String getJdbcConnectionString();
    void setJdbcConnectionString(String value);
    String getDbDriver();
    void setDbDriver(String value);
    String getDbUser();
    void setDbUser(String value);
    String getDbPwd();
    void setDbPwd(String value);
    String getDbTestSchema();
    void setDbTestSchema(String value);
    String getDbImportTable();
    void setDbImportTable(String value);
    String getDbExportTable();
    void setDbExportTable(String value);
    String getDbCountAlias();
    void setDbCountAlias(String value);
    String getHdfsHost();
    void setHdfsHost(String value);
    String getHdfsInputPath();
    void setHdfsInputPath(String value);
    String getHdfsOutputPath();
    void setHdfsOutputPath(String value);
    String getSparkMasterHost();
    void setSparkMasterHost(String value);
    String getSparkAppName();
    void setSparkAppName(String value);
    boolean getAdbConnectorUsageValue();
    void setAdbConnectorUsageValue(boolean value);
    int getSliceDelta();
    void setSliceDelta(int value) throws CustomException;
    EToolAction getToolAction();
    void setToolAction(String value) throws CustomException;
}
