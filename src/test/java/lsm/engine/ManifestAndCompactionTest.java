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

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

