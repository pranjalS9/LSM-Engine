package lsm.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public interface SSTableReader extends Closeable {
    LookupResult lookup(byte[] key) throws IOException;

    default Optional<byte[]> get(byte[] key) throws IOException {
        return switch (lookup(key)) {
            case LookupResult.Found f -> Optional.of(f.value()); //Key exists, here are its bytes
            case LookupResult.Deleted d -> Optional.empty(); //Key has a tombstone — was explicitly deleted
            case LookupResult.NotFound n -> Optional.empty(); //Key isn't in this SSTable
        };
        /**
         *  This distinction matters during multi-level reads.
         *  If you find Deleted in a newer SSTable, you must stop searching older SSTables — the key is gone.
         *  If you find NotFound, you must keep searching older levels.
         */
    }

    @Override
    void close() throws IOException;
}

/**
 * lookup() — the raw result, exposes all three states.
 *      - Used by the engine's read path when searching across multiple SSTable levels.
 * get() — convenience default method, collapses to Optional<byte[]>.
 *      - Useful when you only care about the final value and don't need to distinguish Deleted from NotFound (e.g. single-level reads, tests).
 */

