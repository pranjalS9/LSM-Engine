package lsm.engine;
import java.nio.file.Path;
public record EngineConfig(

        //Where the engine stores SSTable files on disk
        Path dataDir,

        //The flush threshold
        long memtableMaxBytes
) {
    public EngineConfig {
        if (dataDir == null) throw new IllegalArgumentException("dataDir cannot be null");
        if (memtableMaxBytes <= 0) throw new IllegalArgumentException("memtableMaxBytes must be > 0");
    }
}