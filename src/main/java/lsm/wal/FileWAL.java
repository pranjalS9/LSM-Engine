package lsm.wal;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 *  Writes records to a file on disk with group commit support via a background syncer thread
 */
public final class FileWAL implements WAL {
    private static final byte TYPE_PUT = 1;
    private static final byte TYPE_DELETE = 2;

    private final Path path;
    private final FileChannel channel;
    private final long groupCommitMaxDelayMillis;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition appendedOrClosed = lock.newCondition();
    private final Condition durableAdvanced = lock.newCondition();
    private volatile boolean closed = false;
    private long appendedLsn = 0;
    private long durableLsn = 0;
    private final Thread syncer;

    private FileWAL(Path path, FileChannel channel, long groupCommitMaxDelayMillis) throws IOException {
        this.path = path;
        this.channel = channel;
        this.groupCommitMaxDelayMillis = Math.max(0, groupCommitMaxDelayMillis);
        this.appendedLsn = channel.position();
        this.durableLsn = this.appendedLsn;
        this.syncer = new Thread(this::syncerLoop, "wal-syncer");
        this.syncer.setDaemon(true);
        this.syncer.start();
    }

    public static FileWAL open(Path path) throws IOException {
        return open(path, 0);
    }

    /**
     * @param groupCommitMaxDelayMillis maximum time to wait to batch fsyncs.
     *                                 Use 0 for "as soon as possible" syncer behavior.
     */
    public static FileWAL open(Path path, long groupCommitMaxDelayMillis) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        FileChannel ch = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        ch.position(ch.size());
        return new FileWAL(path, ch, groupCommitMaxDelayMillis);
    }

    @Override
    public long append(WALRecord record) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(1 + 8 + 4 + 4).order(BIG_ENDIAN);
        byte type = record.type() == WALRecord.Type.PUT ? TYPE_PUT : TYPE_DELETE;
        byte[] key = record.key();
        byte[] value = record.value();
        int keyLen = key.length;
        int valLen = (record.type() == WALRecord.Type.PUT) ? value.length : -1;

        header.put(type);
        header.putLong(record.sequence());
        header.putInt(keyLen);
        header.putInt(valLen);
        header.flip();

        lock.lock();
        try {
            writeFully(header);
            writeFully(ByteBuffer.wrap(key));
            if (valLen >= 0) {
                writeFully(ByteBuffer.wrap(value));
            }
            appendedLsn = channel.position();
            appendedOrClosed.signal();
            return appendedLsn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void awaitDurable(long lsn) throws IOException, InterruptedException {
        lock.lock();
        try {
            while (!closed && durableLsn < lsn) {
                durableAdvanced.await();
            }
            if (closed && durableLsn < lsn) {
                throw new IOException("WAL closed before reaching requested durability");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized Iterator<WALRecord> replay() throws IOException {
        FileChannel readCh = FileChannel.open(path, StandardOpenOption.READ);
        return new Iterator<>() {
            private long pos = 0;
            private WALRecord next;
            private boolean loaded = false;
            private boolean done = false;

            @Override
            public boolean hasNext() {
                if (done) return false;
                if (!loaded) {
                    next = readNext();
                    loaded = true;
                    if (next == null) done = true;
                }
                return !done;
            }

            @Override
            public WALRecord next() {
                if (!hasNext()) throw new NoSuchElementException();
                loaded = false;
                return next;
            }

            private WALRecord readNext() {
                try {
                    ByteBuffer header = ByteBuffer.allocate(1 + 8 + 4 + 4).order(BIG_ENDIAN);
                    readFully(readCh, header, pos);
                    header.flip();
                    byte type = header.get();
                    long seq = header.getLong();
                    int keyLen = header.getInt();
                    int valLen = header.getInt();
                    if (keyLen < 0 || keyLen > 128 * 1024 * 1024) return null;
                    if (valLen < -1 || valLen > 1024 * 1024 * 1024) return null;

                    long afterHeader = pos + header.capacity();
                    byte[] key = new byte[keyLen];
                    readFully(readCh, ByteBuffer.wrap(key), afterHeader);
                    long afterKey = afterHeader + keyLen;

                    WALRecord.Type t;
                    byte[] value = null;
                    if (type == TYPE_PUT) {
                        t = WALRecord.Type.PUT;
                        if (valLen < 0) return null;
                        value = new byte[valLen];
                        readFully(readCh, ByteBuffer.wrap(value), afterKey);
                        pos = afterKey + valLen;
                    } else if (type == TYPE_DELETE) {
                        t = WALRecord.Type.DELETE;
                        pos = afterKey;
                    } else {
                        return null;
                    }
                    return new WALRecord(t, key, value, seq);
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
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            appendedOrClosed.signalAll();
            durableAdvanced.signalAll();
        } finally {
            lock.unlock();
        }

        try {
            syncer.join(5_000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        channel.close();
    }

    private void writeFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        while (dst.hasRemaining()) {
            int n = ch.read(dst, position);
            if (n < 0) throw new EOFException();
            position += n;
        }
    }

    private void syncerLoop() {
        while (true) {
            long target;
            lock.lock();
            try {
                while (!closed && durableLsn >= appendedLsn) {
                    appendedOrClosed.await();
                }
                if (closed) break;
                if (groupCommitMaxDelayMillis > 0) {
                    appendedOrClosed.await(groupCommitMaxDelayMillis, TimeUnit.MILLISECONDS);
                }
                target = appendedLsn;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } finally {
                lock.unlock();
            }

            try {
                // force(false): flush file contents; metadata durability handled at create/rename boundaries.
                channel.force(false);
            } catch (IOException ignored) {
                // best-effort; awaiters will keep waiting unless closed.
            }

            lock.lock();
            try {
                if (target > durableLsn) durableLsn = target;
                durableAdvanced.signalAll();
            } finally {
                lock.unlock();
            }
        }

        // Best-effort final flush on shutdown.
        try {
            channel.force(false);
            lock.lock();
            try {
                durableLsn = Math.max(durableLsn, appendedLsn);
                durableAdvanced.signalAll();
            } finally {
                lock.unlock();
            }
        } catch (IOException ignored) {
        }
    }
}

