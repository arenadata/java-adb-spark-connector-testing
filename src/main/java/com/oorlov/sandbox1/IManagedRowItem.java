package com.oorlov.sandbox1;
import java.sql.Date;

public interface IManagedRowItem {
    int getId();
    void setId(int value);
    Date getDatetime();
    void setDatetime(Date value);
    String getValue();
    void setValue(String value);
    int getPart();
    void setPart(int value);
}
