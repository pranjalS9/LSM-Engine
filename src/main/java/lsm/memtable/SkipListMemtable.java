package lsm.memtable;

import lsm.common.ByteArrayComparator;
import lsm.common.Value;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SkipListMemtable implements Memtable {
    /*
    ConcurrentSkipListMap gives:
        - Sorted order — iterates in ByteArrayComparator order, ready for SSTable flush
        - Thread safety — multiple threads can read/write concurrently without explicit locking
        - O(log n) average for put/get
     */

    //ByteArrayComparator.INSTANCE passed at construction — the map uses it for all ordering decisions.
    private final ConcurrentSkipListMap<byte[], Value> map = new ConcurrentSkipListMap<>(ByteArrayComparator.INSTANCE);
    private final AtomicLong approximateBytes = new AtomicLong();

    @Override
    public void put(byte[] key, Value value) {
        byte[] k = ByteArrayComparator.copy(key);
        Value prev = map.put(k, value);
        long delta = (k == null ? 0 : k.length) + (value.valueBytes() == null ? 0 : value.valueBytes().length);
        if (prev != null) {
            delta -= (prev.valueBytes() == null ? 0 : prev.valueBytes().length);
        }
        approximateBytes.addAndGet(Math.max(delta, 0));
    }

    @Override
    public Optional<Value> get(byte[] key) {
        Value v = map.get(key);
        return Optional.ofNullable(v);
    }

    @Override
    public long approximateBytes() {
        return approximateBytes.get();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Iterator<Map.Entry<byte[], Value>> iterator() {
        return map.entrySet().iterator();
    }
}

