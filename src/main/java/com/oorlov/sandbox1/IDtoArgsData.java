package com.oorlov.sandbox1;

public interface IDtoArgsData {
    String getJdbcConnectionString();
    void setJdbcConnectionString(String value) throws Exception;
    String getDbDriver();
    void setDbDriver(String value) throws Exception;
    String getDbUser();
    void setDbUser(String value) throws Exception;
    String getDbPwd();
    void setDbPwd(String value) throws Exception;
    String getDbTestSchema();
    void setDbTestSchema(String value) throws Exception;
    String getDbImportTable();
    void setDbImportTable(String value) throws Exception;
    String getDbExportTable();
    void setDbExportTable(String value) throws Exception;
    String getDbCountAlias();
    void setDbCountAlias(String value) throws Exception;
    String getHdfsHost();
    void setHdfsHost(String value) throws Exception;
    String getHdfsInputPath();
    void setHdfsInputPath(String value) throws Exception;
    String getHdfsOutputPath();
    void setHdfsOutputPath(String value) throws Exception;
    String getSparkMasterHost();
    void setSparkMasterHost(String value) throws Exception;
    String getSparkAppName();
    void setSparkAppName(String value) throws Exception;
    boolean getAdbConnectorUsageValue();
    void setAdbConnectorUsageValue(boolean value);
    int getSliceDelta();
    void setSliceDelta(int value) throws Exception;
    EToolAction getToolAction();
    void setToolAction(String value) throws Exception;
}
