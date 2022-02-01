package com.oorlov.sandbox1;
import java.util.List;
import org.apache.hadoop.fs.Path;

public interface IParquetCollection {
    Object createCopy();
    String getParentName();
    void setParentName(String newParentName);
    List<Path> getFiles();
    void setFiles(List<Path> newFiles);
}
