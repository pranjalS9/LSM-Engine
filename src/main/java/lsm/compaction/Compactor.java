package lsm.compaction;

import java.io.Closeable;
import java.io.IOException;

/**
 * For merging and cleaning up SSTable files on disk
 *
 * Every flush() creates a new SSTable. Over time:
 *   - Hundreds of SSTable files accumulate
 *   - The same key may exist in multiple files (old value + newer overwrite + tombstone)
 *   - Reads must check more and more files
 *   - Disk space grows with obsolete versions
 *
 *   Compaction merges SSTables, keeps only the latest version of each key, and discards tombstoned keys —
 *   reclaiming space and restoring read performance.
 */
public interface Compactor extends Closeable {

    /**
     * The compactor decides internally whether compaction is actually needed right now
     * The engine calls this periodically or after each flush, and the compactor applies its own policy:
     *   - Too few SSTables? Do nothing.
     *   - L0 has ≥ 4 files? Compact L0 → L1.
     *   - L1 exceeds size threshold? Compact L1 → L2
     */
    void maybeCompact() throws IOException;

    @Override
    void close() throws IOException;
}

