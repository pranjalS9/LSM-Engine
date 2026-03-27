package lsm.memtable;

import lsm.common.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

//in-memory write buffer — the first place every write lands before hitting disk.
public interface Memtable {
    void put(byte[] key, Value value);

    //Full Value is required to check isTombstone() and sequence(), not just the raw bytes
    Optional<Value> get(byte[] key);

    /*Tracks memory usage.
    - LSM engines flush the MemTable to disk when it exceeds a size threshold (e.g. 64MB).
    - This gives a running estimate without exact accounting — "approximate" because it may not count object overhead,
      just key + value byte lengths.
     */
    long approximateBytes();

    //Number of entries (key-value pairs) currently in the MemTable.
    int size();

    //Returns entries as Map.Entry<byte[], Value> in sorted key order — required for flush.
    //The SSTable written during flush must be sorted, so the iterator must iterate in ByteArrayComparator order.
    Iterator<Map.Entry<byte[], Value>> iterator();
}

