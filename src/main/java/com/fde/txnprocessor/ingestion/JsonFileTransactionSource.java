package com.fde.txnprocessor.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fde.txnprocessor.model.TransactionRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Reads a JSON array of TransactionRequest from a classpath resource or an
 * absolute/relative filesystem path. Classpath lookup is tried first so the
 * bundled sample ships inside the jar; filesystem fallback supports arbitrary
 * inputs the operator supplies at runtime.
 */
public class JsonFileTransactionSource implements TransactionSource {

    private final String location;
    private final ObjectMapper mapper;

    public JsonFileTransactionSource(String location) {
        this.location = location;
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public List<TransactionRequest> read() {
        try (InputStream classpath = getClass().getClassLoader().getResourceAsStream(location)) {
            if (classpath != null) {
                return Arrays.asList(mapper.readValue(classpath, TransactionRequest[].class));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading classpath resource " + location, e);
        }
        Path path = Path.of(location);
        try (InputStream fs = Files.newInputStream(path)) {
            return Arrays.asList(mapper.readValue(fs, TransactionRequest[].class));
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading file " + path, e);
        }
    }
}
