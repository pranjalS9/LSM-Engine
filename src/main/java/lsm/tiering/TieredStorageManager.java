package lsm.tiering;

import java.io.Closeable;
import java.io.IOException;

/**
 *  Manages moving cold SSTables from fast local storage to slower, cheaper remote storage.
 *  On a cache miss, they're fetched back on demand.
 */
public interface TieredStorageManager extends Closeable {
    void maybeTierColdData() throws IOException;

    @Override
    void close() throws IOException;
}

