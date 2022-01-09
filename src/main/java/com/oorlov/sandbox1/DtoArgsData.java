package com.oorlov.sandbox1;
import org.apache.commons.lang.NullArgumentException;

public class DtoArgsData implements IDtoArgsData {
    private String  _jdbcConnectionString;
    private String  _dbDriver;
    private String  _dbUser;
    private String  _dbPwd;
    private String  _dbTestSchema;
    private String  _dbTestTable;
    private String  _dbCountAlias;
    private String  _hdfsOutputPath;
    private String  _sparkMasterHost;
    private String  _sparkAppName;
    private boolean _useAdbConnector = false;
    private int     _sliceDelta = -1;
    private final int DEFAULT_MULTIPLE_VALUE = 100;

    private void checkInputString(String value) throws Exception {
        if (value == null)
            throw new NullArgumentException("The given string object can't be null");

        if (value.length() == 0)
            throw new Exception("The given string value mustn't be equal zero.");
    }

    public String getJdbcConnectionString() {
        return this._jdbcConnectionString;
    }

    public void setJdbcConnectionString(String value) throws Exception {
        checkInputString(value);
        this._jdbcConnectionString = value;
    }

    public String getDbDriver() {
        return this._dbDriver;
    }

    public void setDbDriver(String value) throws Exception {
        checkInputString(value);
        this._dbDriver = value;
    }

    public String getDbUser() {
        return this._dbUser;
    }

    public void setDbUser(String value) throws Exception {
        checkInputString(value);
        this._dbUser = value;
    }

    public String getDbPwd() {
        return this._dbPwd;
    }

    public void setDbPwd(String value) throws Exception {
        checkInputString(value);
        this._dbPwd = value;
    }

    public String getDbTestSchema() {
        return this._dbTestSchema;
    }

    public void setDbTestSchema(String value) throws Exception {
        checkInputString(value);
        this._dbTestSchema = value;
    }

    public String getDbTestTable() {
        return this._dbTestTable;
    }

    public void setDbTestTable(String value) throws Exception {
        checkInputString(value);
        this._dbTestTable = value;
    }

    public String getDbCountAlias() {
        return this._dbCountAlias;
    }

    public void setDbCountAlias(String value) throws Exception {
        checkInputString(value);
        this._dbCountAlias = value;
    }

    public String getHdfsOutputPath() {
        return this._hdfsOutputPath;
    }

    public void setHdfsOutputPath(String value) throws Exception {
        checkInputString(value);
        this._hdfsOutputPath = value;
    }

    public String getSparkMasterHost() {
        return this._sparkMasterHost;
    }

    public void setSparkMasterHost(String value) throws Exception {
        checkInputString(value);
        this._sparkMasterHost = value;
    }

    public String getSparkAppName() {
        return this._sparkAppName;
    }

    public void setSparkAppName(String value) throws Exception {
        checkInputString(value);
        this._sparkAppName = value;
    }

    public void setAdbConnectorUsageValue(boolean value) { this._useAdbConnector = value; }

    public boolean getAdbConnectorUsageValue() { return this._useAdbConnector; }

    public int getSliceDelta() {
        return this._sliceDelta;
    }

    public void setSliceDelta(int value) throws Exception {
        if (value % DEFAULT_MULTIPLE_VALUE != 0)
            throw new Exception(String.format("The input integer value isn't multiple of %s.", DEFAULT_MULTIPLE_VALUE));

        this._sliceDelta = value;
    }
}
