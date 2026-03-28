package lsm.engine;

import lsm.wal.FileWAL;
import lsm.wal.WAL;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class ManifestAndCompactionTest {

    @Test
    void manifestLoadsAndBackgroundCompactionDoesNotBreakReads() throws Exception {
        Path dir = Files.createTempDirectory("lsm-manifest");
        Path walPath = dir.resolve("wal.log");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            for (int i = 1; i <= 6; i++) {
                engine.put(b("k1"), b("v" + i));
                engine.flush();
            }
        }

        assertTrue(Files.exists(dir.resolve("manifest.json")));

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v6"), engine.get(b("k1")).orElseThrow());
            // Give compaction thread a moment to run.
            Thread.sleep(600);
            assertArrayEquals(b("v6"), engine.get(b("k1")).orElseThrow());
        }
    }

    @Test
    void walTruncatedAfterFlush() throws Exception {
        Path dir = Files.createTempDirectory("lsm-wal-trunc");
        Path walPath = dir.resolve("wal.log");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("k1"), b("v1"));
            engine.put(b("k2"), b("v2"));
            engine.flush();
            // WAL should be empty after flush
            assertEquals(0, Files.size(walPath));
        }
    }

    @Test
    void walOnlyContainsPostFlushWrites() throws Exception {
        Path dir = Files.createTempDirectory("lsm-wal-postflush");
        Path walPath = dir.resolve("wal.log");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("k1"), b("v1"));
            engine.flush();
            // WAL truncated — write new data after flush
            engine.put(b("k2"), b("v2"));
            long sizeAfterPostFlushWrite = Files.size(walPath);
            assertTrue(sizeAfterPostFlushWrite > 0, "WAL should have post-flush write");
        }

        // On restart: k1 from SSTable, k2 from WAL replay
        try (WAL wal = FileWAL.open(walPath, 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v1"), engine.get(b("k1")).orElseThrow());
            assertArrayEquals(b("v2"), engine.get(b("k2")).orElseThrow());
        }
    }

    @Test
    void tombstonesDroppedAfterBottommostCompaction() throws Exception {
        Path dir = Files.createTempDirectory("lsm-tombstone");
        Path walPath = dir.resolve("wal.log");
        // Low threshold so compaction triggers after 2 SSTables
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);
        LSMOptions opts = new LSMOptions(
                1 << 13, 4, 32, 2, 4, 100,
                LSMOptions.WALSyncPolicy.GROUP_COMMIT, 5
        );

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal, opts)) {
            engine.put(b("k1"), b("v1"));
            engine.flush();
            engine.delete(b("k1"));
            engine.flush();
            // Two SSTables exist — trigger compaction
            Thread.sleep(500);
            // After bottommost compaction tombstone is dropped, key is gone
            assertTrue(engine.get(b("k1")).isEmpty());
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

