package com.oorlov.sandbox1;
import java.util.List;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.fs.Path;

public class ParquetCollection implements IParquetCollection {
    private String parentName = null;
    private List<Path> files  = null;

    public Object createCopy() {
        ParquetCollection collection = new ParquetCollection();
        collection.parentName        = this.parentName;
        collection.files             = this.files;
        return collection;
    }

    public String getParentName() { return parentName; }

    public void setParentName(String newParentName) {
        IDataHandler.checkInputString(newParentName);
        parentName = newParentName;
    }

    public List<Path> getFiles() { return files; }

    public void setFiles(List<Path> newFiles) {
        if (newFiles == null || newFiles.isEmpty())
            throw new NullArgumentException("The given list with file-paths can't be null and its size can't be equal zero.");

        files = newFiles;
    }
}
