package com.oorlov.sandbox1;
import java.sql.Date;

public class ManagedRowItem implements IManagedRowItem {
    // Consts, which describe the field order, saved in parquet-file
    public static final int PARQUET_POSITION_FIELD_ID         = 0;
    public static final int PARQUET_POSITION_FIELD_DATETIME   = 1;
    public static final int PARQUET_POSITION_FIELD_VALUE      = 2;
    public static final int PARQUET_POSITION_FIELD_PART       = 3;
    // Consts, which describe the SQL prepared statement order
    public static final int STATEMENT_POSITION_FIELD_ID       = 1;
    public static final int STATEMENT_POSITION_FIELD_DATETIME = 2;
    public static final int STATEMENT_POSITION_FIELD_VALUE    = 3;
    public static final int STATEMENT_POSITION_FIELD_PART     = 4;

    private int    _id;
    private Date   _datetime;
    private String _value;
    private int    _part;

    public int getId() {
        return this._id;
    }

    public void setId(int value) {
        this._id = value;
    }

    public Date getDatetime() { return this._datetime; }

    public void setDatetime(Date value) {
        this._datetime = value;
    }

    public String getValue() { return this._value; }

    public void setValue(String value) throws Exception {
        IDataHandler.checkInputString(value);
        this._value = value;
    }

    public int getPart() { return this._part; }

    public void setPart(int value) {
        this._part = value;
    }
}
