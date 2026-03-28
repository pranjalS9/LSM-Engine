package lsm.engine;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

// public contract for the entire LSM Engine

/*
 Closeable -
    - guarantees the engine is closed even if an exception occurs
    - Signals that the engine holds resources (file handles, memory) that must be released
 */
public interface StorageEngine extends Closeable {

    /*
    Why byte[]?
        - byte[] is just raw memory — no encoding assumptions, store anything.
        -  byte[] is the lowest common denominator. The engine doesn't care what the bytes mean —
           it just stores and retrieves them efficiently. Anything built on top can add its own type layer.
     */

    // Write/Update a key-value pair
    void put(byte[] key, byte[] value) throws IOException;

    // Read a value; returns Optional.empty() if key doesn't exist
    // preventing NullPointerExceptions
    Optional<byte[]> get(byte[] key) throws IOException;

    // Mark a key as deleted (LSM uses tombstones, not immediate removal)
    void delete(byte[] key) throws IOException;

    // Returns all key-value pairs where startKey <= key <= endKey, in sorted order.
    // Null startKey means scan from the beginning; null endKey means scan to the end.
    // Tombstoned (deleted) keys are not returned.
    default Iterator<Map.Entry<byte[], byte[]>> scan(byte[] startKey, byte[] endKey) throws IOException {
        throw new UnsupportedOperationException("scan not implemented");
    }

    // Force the in-memory buffer (MemTable) to be written to disk as an SSTable
    /*
      - The MemTable is an in-memory sorted data structure (typically a skip list or red-black tree) where all writes
        land first. It's fast because it's just RAM writes.
      - But RAM is volatile — a crash loses everything. And it can't grow forever. So at some point,
        the MemTable needs to be written to disk as an SSTable (Sorted String Table). That's what flush() does.
     */
    void flush() throws IOException;

    // Gracefully shut down the engine, flush pending data, release resources
    // typically calls flush internally as part of cleanup
    @Override
    void close() throws IOException;
}
