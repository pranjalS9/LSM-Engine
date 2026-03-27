package lsm.wal;

import lsm.engine.EngineConfig;
import lsm.engine.StorageEngine;
import lsm.engine.WALStorageEngine;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

public class WALRecoveryTest {

    @Test
    void recoversAfterRestart() throws Exception {
        Path dir = Files.createTempDirectory("lsm-wal-test");
        Path walPath = dir.resolve("wal.log");

        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);
        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new WALStorageEngine(cfg, wal)) {
            engine.put(b("k1"), b("v1"));
            engine.put(b("k2"), b("v2"));
            engine.delete(b("k2"));
        }

        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new WALStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v1"), engine.get(b("k1")).orElseThrow());
            assertTrue(engine.get(b("k2")).isEmpty());
        }
    }

    @Test
    void toleratesTruncatedTailRecord() throws Exception {
        Path dir = Files.createTempDirectory("lsm-wal-trunc");
        Path walPath = dir.resolve("wal.log");

        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);
        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new WALStorageEngine(cfg, wal)) {
            engine.put(b("k1"), b("v1"));
            engine.put(b("k2"), b("v2"));
        }

        long size = Files.size(walPath);
        Files.newByteChannel(walPath, StandardOpenOption.WRITE).truncate(Math.max(0, size - 5)).close();

        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new WALStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v1"), engine.get(b("k1")).orElseThrow());
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

