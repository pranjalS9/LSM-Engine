package lsm.engine;

import lsm.wal.FileWAL;
import lsm.wal.WAL;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RangeScanTest {

    @Test
    void scanReturnsAllKeysInRange() throws Exception {
        Path dir = Files.createTempDirectory("lsm-scan");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("a"), b("1"));
            engine.put(b("b"), b("2"));
            engine.put(b("c"), b("3"));
            engine.put(b("d"), b("4"));
            engine.put(b("e"), b("5"));

            List<String> keys = scanKeys(engine, b("b"), b("d"));
            assertEquals(List.of("b", "c", "d"), keys);
        }
    }

    @Test
    void scanSkipsTombstones() throws Exception {
        Path dir = Files.createTempDirectory("lsm-scan-tomb");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("a"), b("1"));
            engine.put(b("b"), b("2"));
            engine.put(b("c"), b("3"));
            engine.delete(b("b"));

            List<String> keys = scanKeys(engine, null, null);
            assertEquals(List.of("a", "c"), keys);
        }
    }

    @Test
    void scanAcrossMemtableAndSSTables() throws Exception {
        Path dir = Files.createTempDirectory("lsm-scan-multi");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            // First batch → SSTable
            engine.put(b("a"), b("1"));
            engine.put(b("c"), b("3"));
            engine.flush();

            // Second batch → MemTable
            engine.put(b("b"), b("2"));
            engine.put(b("d"), b("4"));

            List<String> keys = scanKeys(engine, null, null);
            assertEquals(List.of("a", "b", "c", "d"), keys);
        }
    }

    @Test
    void scanRespectsOverwrite() throws Exception {
        Path dir = Files.createTempDirectory("lsm-scan-overwrite");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("k"), b("old"));
            engine.flush();
            engine.put(b("k"), b("new"));

            Iterator<Map.Entry<byte[], byte[]>> it = engine.scan(null, null);
            assertTrue(it.hasNext());
            Map.Entry<byte[], byte[]> e = it.next();
            assertArrayEquals(b("k"), e.getKey());
            assertArrayEquals(b("new"), e.getValue());
            assertFalse(it.hasNext());
        }
    }

    @Test
    void scanNullBoundsReturnsAll() throws Exception {
        Path dir = Files.createTempDirectory("lsm-scan-all");
        EngineConfig cfg = new EngineConfig(dir, 1024 * 1024);

        try (WAL wal = FileWAL.open(dir.resolve("wal.log"), 0);
             StorageEngine engine = new LSMStorageEngine(cfg, wal)) {
            engine.put(b("x"), b("1"));
            engine.put(b("y"), b("2"));
            engine.put(b("z"), b("3"));

            List<String> keys = scanKeys(engine, null, null);
            assertEquals(List.of("x", "y", "z"), keys);
        }
    }

    private static List<String> scanKeys(StorageEngine engine, byte[] start, byte[] end) throws Exception {
        List<String> keys = new ArrayList<>();
        Iterator<Map.Entry<byte[], byte[]>> it = engine.scan(start, end);
        while (it.hasNext()) {
            keys.add(new String(it.next().getKey(), StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}