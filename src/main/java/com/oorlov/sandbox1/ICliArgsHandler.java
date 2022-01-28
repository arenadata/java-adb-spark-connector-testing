package com.oorlov.sandbox1;

public interface ICliArgsHandler {
    DtoArgsData createManagedDto(String[] cliRawArgs) throws CustomException;
}
