package lsm.engine;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryStorageEngineTest {

    @Test
    void putGetRoundTrip() throws Exception {
        EngineConfig cfg = new EngineConfig(Path.of("data"), 1024 * 1024);
        try (StorageEngine engine = new InMemoryStorageEngine(cfg)) {
            engine.put(b("k1"), b("v1"));
            Optional<byte[]> got = engine.get(b("k1"));
            assertTrue(got.isPresent());
            assertArrayEquals(b("v1"), got.get());
        }
    }

    @Test
    void overwriteKeepsLatest() throws Exception {
        EngineConfig cfg = new EngineConfig(Path.of("data"), 1024 * 1024);
        try (StorageEngine engine = new InMemoryStorageEngine(cfg)) {
            engine.put(b("k1"), b("v1"));
            engine.put(b("k1"), b("v2"));
            assertArrayEquals(b("v2"), engine.get(b("k1")).orElseThrow());
        }
    }

    @Test
    void deleteCreatesTombstone() throws Exception {
        EngineConfig cfg = new EngineConfig(Path.of("data"), 1024 * 1024);
        try (StorageEngine engine = new InMemoryStorageEngine(cfg)) {
            engine.put(b("k1"), b("v1"));
            engine.delete(b("k1"));
            assertTrue(engine.get(b("k1")).isEmpty());
        }
    }

    @Test
    void getMissingKeyReturnsEmpty() throws Exception {
        EngineConfig cfg = new EngineConfig(Path.of("data"), 1024 * 1024);
        try (StorageEngine engine = new InMemoryStorageEngine(cfg)) {
            assertTrue(engine.get(b("nonexistent")).isEmpty());
        }
    }

    @Test
    void operationsAfterCloseThrow() throws Exception {
        EngineConfig cfg = new EngineConfig(Path.of("data"), 1024 * 1024);
        StorageEngine engine = new InMemoryStorageEngine(cfg);
        engine.close();
        assertThrows(IOException.class, () -> engine.put(b("k1"), b("v1")));
        assertThrows(IOException.class, () -> engine.get(b("k1")));
        assertThrows(IOException.class, () -> engine.delete(b("k1")));
    }

    //Explicit StandardCharsets.UTF_8 instead of platform default.
    //Tests produce the same bytes on every machine regardless of OS locale.
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

