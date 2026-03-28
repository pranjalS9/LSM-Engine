package lsm.sstable;

import lsm.common.Value;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * The interface for writing a MemTable to disk as an SSTable file.
 */
public interface SSTableWriter extends Closeable {

    /**
     *  writeAll(.....)
     *  Takes exactly what SkipListMemtable.iterator() produces — a Map.Entry<byte[], Value> iterator in sorted key order.
     *  The contract is that entries arrive pre-sorted — the writer just serializes them sequentially, no sorting needed.
     *  Accepts Value (not raw byte[]) so the writer can handle both regular entries and tombstones.
     *  Tombstones need to be written to disk so compaction and reads across levels know a key was deleted.
     */
    void writeAll(Iterator<Map.Entry<byte[], Value>> sortedEntries) throws IOException;

    @Override
    void close() throws IOException;
}

