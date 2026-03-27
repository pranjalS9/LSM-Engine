package lsm.engine;

import lsm.common.ByteArrayComparator;
import lsm.common.Value;
import lsm.memtable.Memtable;
import lsm.memtable.SkipListMemtable;
import lsm.wal.WAL;
import lsm.wal.WALRecord;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class WALStorageEngine implements StorageEngine {
    private final EngineConfig config;
    private final WAL wal;
    private final AtomicLong sequence = new AtomicLong(0);
    private final Memtable memtable = new SkipListMemtable();
    private volatile boolean closed = false;

    public WALStorageEngine(EngineConfig config, WAL wal) throws IOException {
        this.config = config;
        this.wal = wal;
        recoverFromWal();
    }

    private void recoverFromWal() throws IOException {
        Iterator<WALRecord> it = wal.replay();
        long maxSeq = 0;
        while (it.hasNext()) {
            WALRecord r = it.next();
            maxSeq = Math.max(maxSeq, r.sequence());
            if (r.type() == WALRecord.Type.PUT) {
                memtable.put(r.key(), Value.put(r.value(), r.sequence()));
            } else {
                memtable.put(r.key(), Value.tombstone(r.sequence()));
            }
        }
        sequence.set(maxSeq);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        ensureOpen();
        long seq = sequence.incrementAndGet();
        long lsn = wal.append(new WALRecord(WALRecord.Type.PUT, key, value, seq));
        try {
            wal.awaitDurable(lsn);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for WAL durability", ie);
        }
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
        long lsn = wal.append(new WALRecord(WALRecord.Type.DELETE, key, null, seq));
        try {
            wal.awaitDurable(lsn);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for WAL durability", ie);
        }
        memtable.put(ByteArrayComparator.copy(key), Value.tombstone(seq));
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        // no-op until SSTables are implemented
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        wal.close();
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("engine is closed");
    }
}

