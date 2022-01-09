package com.oorlov.sandbox1;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Main {
    private static final Logger _logger = LogManager.getRootLogger();

    public static void main(String[] args) {
        try {
            ICliArgsHandler argsHandler = new CliArgsHandler(_logger);
            IDtoArgsData dtoCliData     = argsHandler.createManagedDto(args);
            IDataHandler dataHandler    = new DataHandler(_logger, dtoCliData);
            dataHandler.startTransaction();
        } catch (Exception exception) {
            _logger.error(exception);
            exception.printStackTrace();
        }
    }
}
