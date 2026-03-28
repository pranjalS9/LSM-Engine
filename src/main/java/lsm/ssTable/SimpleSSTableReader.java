package lsm.sstable;

import lsm.common.ByteArrayComparator;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * Reads from a single SSTable file — loads the sparse index on open, then uses binary search + linear scan to locate keys.
 *
 * Index is loaded into memory immediately on open.
 * The data records stay on disk — only the index (one entry per N records) lives in RAM.
 * This is the sparse index tradeoff in practice
 */

public final class SimpleSSTableReader implements SSTableReader {
    private record IndexEntry(byte[] key, long offset) {}

    private final FileChannel ch;
    private final List<IndexEntry> index;

    public SimpleSSTableReader(Path path) throws IOException {
        this.ch = FileChannel.open(path, StandardOpenOption.READ);
        this.index = loadIndex();
    }

    @Override
    public LookupResult lookup(byte[] key) throws IOException {
        long start = findScanStart(key);
        long pos = start;

        while (true) {
            Entry e = readEntryAt(pos);
            if (e == null) return new LookupResult.NotFound();
            int cmp = ByteArrayComparator.INSTANCE.compare(e.key, key);
            if (cmp == 0) {
                if (e.valLen < 0) return new LookupResult.Deleted();
                return new LookupResult.Found(e.value);
            }
            if (cmp > 0) return new LookupResult.NotFound();
            pos = e.nextOffset;
        }
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    private long findScanStart(byte[] key) {
        if (index.isEmpty()) return 0;
        int lo = 0, hi = index.size() - 1;
        int best = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = ByteArrayComparator.INSTANCE.compare(index.get(mid).key, key);
            if (cmp <= 0) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return index.get(best).offset;
    }

    private List<IndexEntry> loadIndex() throws IOException {
        long size = ch.size();
        if (size < SimpleSSTableFormat.FOOTER_BYTES) return List.of();

        ByteBuffer footer = ByteBuffer.allocate(SimpleSSTableFormat.FOOTER_BYTES).order(BIG_ENDIAN);
        readFully(footer, size - SimpleSSTableFormat.FOOTER_BYTES);
        footer.flip();

        int magic = footer.getInt();
        if (magic != SimpleSSTableFormat.MAGIC) return List.of();
        long indexOffset = footer.getLong();
        int indexCount = footer.getInt();
        if (indexOffset < 0 || indexOffset > size) return List.of();
        if (indexCount < 0 || indexCount > 10_000_000) return List.of();

        List<IndexEntry> out = new ArrayList<>(indexCount);
        long pos = indexOffset;
        for (int i = 0; i < indexCount; i++) {
            ByteBuffer klen = ByteBuffer.allocate(4).order(BIG_ENDIAN);
            readFully(klen, pos);
            klen.flip();
            int keyLen = klen.getInt();
            pos += 4;
            if (keyLen < 0 || keyLen > 128 * 1024 * 1024) break;
            byte[] key = new byte[keyLen];
            readFully(ByteBuffer.wrap(key), pos);
            pos += keyLen;

            ByteBuffer off = ByteBuffer.allocate(8).order(BIG_ENDIAN);
            readFully(off, pos);
            off.flip();
            long offset = off.getLong();
            pos += 8;
            out.add(new IndexEntry(key, offset));
        }
        return out;
    }

    private static final class Entry {
        final byte[] key;
        final int valLen;
        final byte[] value;
        final long nextOffset;

        Entry(byte[] key, int valLen, byte[] value, long nextOffset) {
            this.key = key;
            this.valLen = valLen;
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }

    private Entry readEntryAt(long position) throws IOException {
        try {
            byte[] headerBytes = new byte[4 + 4 + 8];
            readFully(ByteBuffer.wrap(headerBytes), position);
            ByteBuffer header = ByteBuffer.wrap(headerBytes).order(BIG_ENDIAN);
            int keyLen = header.getInt();
            int valLen = header.getInt();
            header.getLong(); // seq
            long pos = position + headerBytes.length;
            if (keyLen < 0 || keyLen > 128 * 1024 * 1024) return null;
            if (valLen < -1 || valLen > 1024 * 1024 * 1024) return null;

            byte[] key = new byte[keyLen];
            readFully(ByteBuffer.wrap(key), pos);
            pos += keyLen;

            byte[] value = null;
            if (valLen >= 0) {
                value = new byte[valLen];
                readFully(ByteBuffer.wrap(value), pos);
                pos += valLen;
            }

            // Verify CRC32
            ByteBuffer crcBuf = ByteBuffer.allocate(4).order(BIG_ENDIAN);
            readFully(crcBuf, pos);
            crcBuf.flip();
            int storedCrc = crcBuf.getInt();
            CRC32 crc = new CRC32();
            crc.update(headerBytes);
            crc.update(key);
            if (valLen >= 0) crc.update(value);
            if ((int) crc.getValue() != storedCrc) {
                throw new IOException("SSTable checksum mismatch at offset " + position);
            }
            pos += 4;

            return new Entry(key, valLen, value, pos);
        } catch (EOFException eof) {
            return null;
        }
    }

    private void readFully(ByteBuffer dst, long position) throws IOException {
        while (dst.hasRemaining()) {
            int n = ch.read(dst, position);
            if (n < 0) throw new EOFException();
            position += n;
        }
    }
}


/**
 *  The sequence number is stored in the file (written by SimpleSSTableWriter) but not used during point lookups —
 *  the MemTable always wins over SSTables for same-key conflicts (higher sequence). Sequence becomes relevant
 *  during compaction when merging two SSTables at the same level.
 */

