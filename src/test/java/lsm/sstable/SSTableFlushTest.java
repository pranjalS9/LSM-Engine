package lsm.sstable;

import lsm.engine.EngineConfig;
import lsm.engine.LSMStorageEngine;
import lsm.engine.StorageEngine;
import lsm.wal.FileWAL;
import lsm.wal.WAL;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class SSTableFlushTest {

    @Test
    void flushPersistsToSSTable() throws Exception {
        Path dir = Files.createTempDirectory("lsm-sstable");
        Path walPath = dir.resolve("wal.log");

        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);
        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("k1"), b("v1"));
            engine.put(b("k2"), b("v2"));
            engine.flush();
        }

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v1"), engine.get(b("k1")).orElseThrow());
            assertArrayEquals(b("v2"), engine.get(b("k2")).orElseThrow());
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

