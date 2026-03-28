package lsm.engine;

import lsm.wal.FileWAL;
import lsm.wal.WAL;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

public final class Bench {
    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 200_000;
        String mode = args.length > 1 ? args[1] : "group";
        long groupDelayMs = args.length > 2 ? Long.parseLong(args[2]) : 5;
        long seed = args.length > 3 ? Long.parseLong(args[3]) : 12345;

        Path dir = Files.createTempDirectory("lsm-bench");
        Path walPath = dir.resolve("wal.log");

        // Keep memtable large so the benchmark isolates WAL behavior (no flush/compaction noise).
        EngineConfig cfg = new EngineConfig(dir, 256L * 1024 * 1024);

        LSMOptions.WALSyncPolicy policy = switch (mode.toLowerCase()) {
            case "strict", "every_write", "everywrite" -> LSMOptions.WALSyncPolicy.EVERY_WRITE;
            case "group", "group_commit", "groupcommit" -> LSMOptions.WALSyncPolicy.GROUP_COMMIT;
            default -> throw new IllegalArgumentException("mode must be strict|group");
        };

        LSMOptions defaults = LSMOptions.defaults();
        LSMOptions opts = new LSMOptions(
                defaults.bloomBits(),
                defaults.bloomK(),
                defaults.sparseIndexEveryN(),
                defaults.compactionThresholdSstables(),
                defaults.keepLocalSstables(),
                defaults.compactionSleepMillis(),
                policy,
                groupDelayMs
        );

        Random rnd = new Random(seed);
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) 'x');

        long startNs = System.nanoTime();
        try (WAL wal = FileWAL.open(walPath, opts.walGroupCommitMaxDelayMillis());
             StorageEngine engine = new LSMStorageEngine(cfg, wal, opts)) {
            for (int i = 0; i < n; i++) {
                // Random-ish keys: 8 bytes from RNG.
                long k = rnd.nextLong();
                byte[] key = new byte[8];
                for (int b = 0; b < 8; b++) {
                    key[b] = (byte) (k >>> (56 - (b * 8)));
                }
                engine.put(key, value);
            }
        }
        long elapsedNs = System.nanoTime() - startNs;
        double secs = elapsedNs / 1e9;
        double opsPerSec = n / secs;
        double avgLatencyUs = (elapsedNs / 1_000.0) / n;

        System.out.printf(
                "mode=%s groupDelayMs=%d puts=%d time=%.3fs ops/sec=%.0f avgLatency=%.2fµs dir=%s%n",
                policy, opts.walGroupCommitMaxDelayMillis(), n, secs, opsPerSec, avgLatencyUs, dir
        );
    }
}

