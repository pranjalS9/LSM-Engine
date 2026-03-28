package lsm.sstable;

import lsm.common.Value;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.nio.ByteOrder.BIG_ENDIAN;

public final class SimpleSSTableScanner implements Iterable<SimpleSSTableScanner.Record>, AutoCloseable {
    public record Record(byte[] key, Value value) {}

    private final FileChannel ch;
    private final long dataEndOffset;

    public SimpleSSTableScanner(Path path) throws IOException {
        this.ch = FileChannel.open(path, StandardOpenOption.READ);
        this.dataEndOffset = readIndexOffset();
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<>() {
            private long pos = 0;
            private Record next;
            private boolean loaded = false;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    next = readNext();
                    loaded = true;
                }
                return next != null;
            }

            @Override
            public Record next() {
                if (!hasNext()) throw new NoSuchElementException();
                loaded = false;
                return next;
            }

            private Record readNext() {
                if (pos >= dataEndOffset) return null;
                try {
                    ByteBuffer header = ByteBuffer.allocate(4 + 4 + 8).order(BIG_ENDIAN);
                    readFully(header, pos);
                    header.flip();
                    int keyLen = header.getInt();
                    int valLen = header.getInt();
                    long seq = header.getLong();
                    long p = pos + header.capacity();
                    if (keyLen < 0) return null;
                    byte[] key = new byte[keyLen];
                    readFully(ByteBuffer.wrap(key), p);
                    p += keyLen;
                    byte[] valueBytes = null;
                    if (valLen >= 0) {
                        valueBytes = new byte[valLen];
                        readFully(ByteBuffer.wrap(valueBytes), p);
                        p += valLen;
                    }
                    pos = p;
                    Value v = (valLen < 0) ? Value.tombstone(seq) : Value.put(valueBytes, seq);
                    return new Record(key, v);
                } catch (EOFException eof) {
                    return null;
                } catch (IOException io) {
                    return null;
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }

    private long readIndexOffset() throws IOException {
        long size = ch.size();
        if (size < SimpleSSTableFormat.FOOTER_BYTES) return size;
        ByteBuffer footer = ByteBuffer.allocate(SimpleSSTableFormat.FOOTER_BYTES).order(BIG_ENDIAN);
        readFully(footer, size - SimpleSSTableFormat.FOOTER_BYTES);
        footer.flip();
        int magic = footer.getInt();
        if (magic != SimpleSSTableFormat.MAGIC) return size;
        long indexOffset = footer.getLong();
        return Math.max(0, Math.min(indexOffset, size));
    }

    private void readFully(ByteBuffer dst, long position) throws IOException {
        while (dst.hasRemaining()) {
            int n = ch.read(dst, position);
            if (n < 0) throw new EOFException();
            position += n;
        }
    }
}

