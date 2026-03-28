package lsm.sstable;

import lsm.bloom.BloomFilter;
import lsm.common.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lsm.bloom.BloomFilter;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * Writes a sorted stream of key-value entries to disk as a complete SSTable file — data records, sparse index, and footer in one pass
 *
 *   File layout:
 *
 *   [record 0: keyLen(4) | valLen(4) | seq(8) | key | value]
 *   [record 1: ...]
 *   [record 2: ...]
 *   ...
 *   [record N: ...]
 *   [index entry 0: keyLen(4) | key | offset(8)]   ← written in close()
 *   [index entry K: ...]
 *   [footer: magic(4) | indexOffset(8) | indexCount(4)]  ← last 16 bytes
 *
 *   Data records first, index after, footer last. Everything BIG_ENDIAN.
 */
public final class SimpleSSTableWriter implements SSTableWriter {
    public record IndexEntry(byte[] key, long offset) {}

    private final FileChannel ch;
    private final List<IndexEntry> sparseIndex = new ArrayList<>();
    private final BloomFilter bloom;
    private final int indexEveryN;
    private int count = 0;
    private boolean closed = false;

    public SimpleSSTableWriter(Path path, int indexEveryN) throws IOException {
        this(path, indexEveryN, null);
    }

    public SimpleSSTableWriter(Path path, int indexEveryN, BloomFilter bloom) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        this.ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
        this.indexEveryN = Math.max(1, indexEveryN);
        this.bloom = bloom;
    }

    @Override
    public void writeAll(Iterator<Map.Entry<byte[], Value>> sortedEntries) throws IOException {
        while (sortedEntries.hasNext()) {
            Map.Entry<byte[], Value> e = sortedEntries.next();
            byte[] key = e.getKey();
            Value v = e.getValue();

            long offset = ch.position();
            if ((count % indexEveryN) == 0) {
                sparseIndex.add(new IndexEntry(key, offset));
            }
            count++;
            if (bloom != null) bloom.add(key);

            byte[] valueBytes = v.isTombstone() ? null : v.valueBytes();
            int keyLen = key.length;
            int valLen = (valueBytes == null) ? -1 : valueBytes.length;

            ByteBuffer header = ByteBuffer.allocate(4 + 4 + 8).order(BIG_ENDIAN);
            header.putInt(keyLen);
            header.putInt(valLen);
            header.putLong(v.sequence());
            header.flip();
            writeFully(header);
            writeFully(ByteBuffer.wrap(key));
            if (valLen >= 0) writeFully(ByteBuffer.wrap(valueBytes));
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;

        long indexOffset = ch.position();
        // index entry: keyLen(int), key bytes, offset(long)
        for (IndexEntry ie : sparseIndex) {
            ByteBuffer klen = ByteBuffer.allocate(4).order(BIG_ENDIAN);
            klen.putInt(ie.key.length);
            klen.flip();
            writeFully(klen);
            writeFully(ByteBuffer.wrap(ie.key));
            ByteBuffer off = ByteBuffer.allocate(8).order(BIG_ENDIAN);
            off.putLong(ie.offset);
            off.flip();
            writeFully(off);
        }

        writeFully(SimpleSSTableFormat.footer(indexOffset, sparseIndex.size()));
        ch.force(true);
        ch.close();
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }
}

