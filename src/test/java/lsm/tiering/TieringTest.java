package lsm.tiering;

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

public class TieringTest {

    @Test
    void movesOldSstablesToRemoteDirectoryAndStillReads() throws Exception {
        Path dir = Files.createTempDirectory("lsm-tier");
        Path walPath = dir.resolve("wal.log");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            for (int i = 1; i <= 8; i++) {
                engine.put(b("k1"), b("v" + i));
                engine.flush();
            }
        }

        Path localDir = dir.resolve("sstables");
        Path remoteDir = dir.resolve("remote-sstables");
        long localCount = Files.list(localDir).filter(p -> p.getFileName().toString().endsWith(".sst")).count();
        long remoteCount = Files.list(remoteDir).filter(p -> p.getFileName().toString().endsWith(".sst")).count();
        assertTrue(remoteCount > 0);
        assertTrue(localCount <= 4);

        try (WAL wal = FileWAL.open(walPath, 5);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            assertArrayEquals(b("v8"), engine.get(b("k1")).orElseThrow());
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

