package lsm.common;

import java.util.Arrays;

public final class Value {
    private final byte[] bytes;      //Actual value data
    private final boolean tombstone; //Deletion marker?
    private final long sequence;     //Version/ordering number (higher sequence = newer write)

    private Value(byte[] bytes, boolean tombstone, long sequence) {
        this.bytes = bytes;
        this.tombstone = tombstone;
        this.sequence = sequence;
    }

    public static Value put(byte[] value, long sequence) {
        return new Value(ByteArrayComparator.copy(value), false, sequence);
    }

    public static Value tombstone(long sequence) {
        return new Value(null, true, sequence);
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public long sequence() {
        return sequence;
    }

    public byte[] valueBytes() {
        return ByteArrayComparator.copy(bytes);
    }

    @Override
    public String toString() {
        return tombstone ? "Value(TOMBSTONE, seq=" + sequence + ")" : "Value(len=" + (bytes == null ? 0 : bytes.length) + ", seq=" + sequence + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Value other)) return false;
        return tombstone == other.tombstone && sequence == other.sequence && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        int result = Boolean.hashCode(tombstone);
        result = 31 * result + Long.hashCode(sequence);
        result = 31 * result + Arrays.hashCode(bytes);
        return result;
    }
}

