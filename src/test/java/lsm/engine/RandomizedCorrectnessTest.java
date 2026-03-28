package lsm.engine;

import lsm.wal.FileWAL;
import lsm.wal.WAL;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class RandomizedCorrectnessTest {

    @Test
    void randomizedOpsMatchHashMapModel() throws Exception {
        Path dir = Files.createTempDirectory("lsm-rand");
        Path walPath = dir.resolve("wal.log");
        EngineConfig cfg = new EngineConfig(dir, 32 * 1024);

        Map<String, byte[]> model = new HashMap<>();
        Random rnd = new Random(12345);

        LSMOptions opts = LSMOptions.defaults();
        try (WAL wal = FileWAL.open(walPath, opts.walGroupCommitMaxDelayMillis());
             StorageEngine engine = new LSMStorageEngine(cfg, wal, opts)) {

            for (int i = 0; i < 2000; i++) {
                String key = "k" + rnd.nextInt(200);
                int op = rnd.nextInt(10);
                if (op < 6) { // put
                    String val = "v" + rnd.nextInt(10_000);
                    engine.put(b(key), b(val));
                    model.put(key, b(val));
                } else if (op < 8) { // delete
                    engine.delete(b(key));
                    model.remove(key);
                } else { // read
                    Optional<byte[]> got = engine.get(b(key));
                    byte[] expected = model.get(key);
                    if (expected == null) {
                        assertTrue(got.isEmpty());
                    } else {
                        assertArrayEquals(expected, got.orElseThrow());
                    }
                }

                if ((i % 200) == 0) engine.flush();
            }
        }

        // Restart and validate a sample of keys.
        try (WAL wal = FileWAL.open(walPath, opts.walGroupCommitMaxDelayMillis());
             StorageEngine engine = new LSMStorageEngine(cfg, wal, opts)) {
            for (int i = 0; i < 200; i++) {
                String key = "k" + i;
                Optional<byte[]> got = engine.get(b(key));
                byte[] expected = model.get(key);
                if (expected == null) assertTrue(got.isEmpty());
                else assertArrayEquals(expected, got.orElseThrow());
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

