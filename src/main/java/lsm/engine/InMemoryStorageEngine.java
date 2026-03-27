package lsm.engine;

import lsm.common.ByteArrayComparator;
import lsm.common.Value;
import lsm.memtable.Memtable;
import lsm.memtable.SkipListMemtable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryStorageEngine implements StorageEngine {
    private final EngineConfig config;

    // Global write counter. Every put and delete increments it and stamps the Value with that sequence number
    private final AtomicLong sequence = new AtomicLong(0);

    //volatile ensures visibility across threads — when the engine eventually swaps the active memtable during flush
    //(freeze current, create new one), other threads immediately see the new reference without stale caching.
    private volatile Memtable memtable = new SkipListMemtable();

    private volatile boolean closed = false;

    public InMemoryStorageEngine(EngineConfig config) {
        this.config = config;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        ensureOpen();
        long seq = sequence.incrementAndGet();
        memtable.put(ByteArrayComparator.copy(key), Value.put(value, seq));
    }

    @Override
    public Optional<byte[]> get(byte[] key) throws IOException {
        ensureOpen();
        Optional<Value> v = memtable.get(key);
        if (v.isEmpty() || v.get().isTombstone()) return Optional.empty();
        return Optional.ofNullable(v.get().valueBytes());
    }

    @Override
    public void delete(byte[] key) throws IOException {
        ensureOpen();
        long seq = sequence.incrementAndGet();
        memtable.put(ByteArrayComparator.copy(key), Value.tombstone(seq));
    }

    //Eventually freeze the current memtable and write it to an SSTable
    @Override
    public void flush() throws IOException {
        ensureOpen();
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    //Guard on every public method. Once closed, the engine rejects all operations
    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("engine is closed");
    }
}

