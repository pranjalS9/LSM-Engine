package lsm.wal;

import lsm.common.ByteArrayComparator;

/**
 *  The unit of data written to the WAL
 */

/**
 * Sequence is critical for replay — when rebuilding the MemTable from WAL on restart,
 * the sequence numbers must match what would have been assigned during normal operation
 * so version ordering stays correct.
 */
public record WALRecord(Type type, byte[] key, byte[] value, long sequence) {
    public enum Type { PUT, DELETE }

    public WALRecord {
        if (type == null) throw new IllegalArgumentException("type cannot be null");
        if (key == null) throw new IllegalArgumentException("key cannot be null");
        if (type == Type.PUT && value == null) throw new IllegalArgumentException("value cannot be null for PUT");
        if (type == Type.DELETE) value = null;
        key = ByteArrayComparator.copy(key);
        value = ByteArrayComparator.copy(value);
    }
}

