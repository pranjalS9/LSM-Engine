package lsm.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

  /**
   * WAL = Write-Ahead Log. Before any write lands in the MemTable, it's first appended to the WAL on disk.
   * If the process crashes, the WAL is replayed on restart to recover writes that were in memory but never flushed to SSTables.
   */
public interface WAL extends Closeable {
    /**
     * Appends a record and returns an LSN(Log Sequence Number) that represents the end offset
     * (or a monotonically increasing durable position) for this write.
     * The LSN is used to track durability — "everything up to LSN X is safe on disk.
     */
    long append(WALRecord record) throws IOException;

    /**
    *  On startup, reads the WAL file from the beginning and replays all records to rebuild the MemTable.
    *  Called once during engine recovery before accepting new writes.\
    */
    Iterator<WALRecord> replay() throws IOException;

    /**
     * Blocks the calling thread until the OS confirms bytes up to lsn are flushed to disk (via fsync or equivalent).
     */
    void awaitDurable(long lsn) throws IOException, InterruptedException;

    /**
     * Forces durability(flushing) of all appended data (best-effort).
     * Prefer {@link #awaitDurable(long)} for group commit.
     */
    default void sync() throws IOException {
    }

    @Override
    void close() throws IOException;
}

