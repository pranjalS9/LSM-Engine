package lsm.engine;

public record LSMOptions(
        int bloomBits,
        int bloomK,
        int sparseIndexEveryN,
        int compactionThresholdSstables,
        int keepLocalSstables,
        long compactionSleepMillis,
        WALSyncPolicy walSyncPolicy,
        long walGroupCommitMaxDelayMillis
) {
    public enum WALSyncPolicy {
        EVERY_WRITE,
        GROUP_COMMIT
    }

    public static LSMOptions defaults() {
        return new LSMOptions(
                1 << 20,
                7,
                32,
                4,
                4,
                250,
                WALSyncPolicy.GROUP_COMMIT,
                5
        );
    }
}

