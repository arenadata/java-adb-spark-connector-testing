package com.oorlov.sandbox1;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;

public class ParquetCollection implements Cloneable {
    public String parentName     = null;
    public ArrayList<Path> files = null;

    protected Object clone() {
        ParquetCollection collection = new ParquetCollection();
        collection.parentName        = this.parentName;
        collection.files             = this.files;
        return collection;
    }
}
