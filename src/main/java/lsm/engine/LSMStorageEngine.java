package lsm.engine;

import lsm.common.ByteArrayComparator;
import lsm.common.Value;
import lsm.memtable.Memtable;
import lsm.memtable.SkipListMemtable;
import lsm.bloom.BloomFilterIO;
import lsm.bloom.SimpleBloomFilter;
import lsm.sstable.SimpleSSTableReader;
import lsm.sstable.SimpleSSTableScanner;
import lsm.sstable.SimpleSSTableWriter;
import lsm.sstable.SSTableReader;
import lsm.tiering.LocalDirectoryTieredStorageManager;
import lsm.tiering.TieredStorageManager;
import lsm.wal.WAL;
import lsm.wal.WALRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class LSMStorageEngine implements StorageEngine {
    private final EngineConfig config;
    private final LSMOptions options;
    private final WAL wal;
    private final Path sstableDir;
    private final AtomicLong sequence = new AtomicLong(0);

    private volatile Memtable memtable = new SkipListMemtable();
    private final List<SSTableHandle> sstablesNewestFirst = new ArrayList<>();
    private volatile boolean closed = false;
    private final Thread compactionThread;
    private final TieredStorageManager tieredStorage;
    private final Path remoteDir;

    public LSMStorageEngine(EngineConfig config, WAL wal) throws IOException {
        this(config, wal, LSMOptions.defaults());
    }

    public LSMStorageEngine(EngineConfig config, WAL wal, LSMOptions options) throws IOException {
        this.config = config;
        this.options = options;
        this.wal = wal;
        this.sstableDir = config.dataDir().resolve("sstables");
        this.remoteDir = config.dataDir().resolve("remote-sstables");
        Files.createDirectories(sstableDir);
        Files.createDirectories(remoteDir);
        this.tieredStorage = new LocalDirectoryTieredStorageManager(remoteDir, options.keepLocalSstables());
        loadFromManifestOrScan();
        recoverFromWal();

        this.compactionThread = new Thread(this::compactionLoop, "lsm-compactor");
        this.compactionThread.setDaemon(true);
        this.compactionThread.start();
    }

    private void loadFromManifestOrScan() throws IOException {
        List<Manifest.Entry> entries = Manifest.load(config.dataDir());
        if (!entries.isEmpty()) {
            for (Manifest.Entry e : entries) {
                Path sst = config.dataDir().resolve(e.sstableRelPath());
                Path bf = e.bloomRelPath() == null ? null : config.dataDir().resolve(e.bloomRelPath());
                SimpleBloomFilter bloom = null;
                if (bf != null && Files.exists(bf)) {
                    bloom = BloomFilterIO.read(bf);
                }
                sstablesNewestFirst.add(new SSTableHandle(sst, bf, e.minKey(), e.maxKey(), bloom));
            }
            return;
        }

        if (!Files.exists(sstableDir)) return;
        try (var stream = Files.list(sstableDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".sst"))
                    .sorted(Comparator.comparing(Path::getFileName).reversed())
                    .forEach(p -> sstablesNewestFirst.add(new SSTableHandle(p, bloomPathFor(p), new byte[0], new byte[0], null)));
        }
    }

    private void recoverFromWal() throws IOException {
        long maxSeq = 0;
        var it = wal.replay();
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
    public synchronized void put(byte[] key, byte[] value) throws IOException {
        ensureOpen();
        long seq = sequence.incrementAndGet();
        long lsn = wal.append(new WALRecord(WALRecord.Type.PUT, key, value, seq));
        if (options.walSyncPolicy() == LSMOptions.WALSyncPolicy.EVERY_WRITE) {
            try {
                wal.awaitDurable(lsn);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for WAL durability", ie);
            }
        }
        memtable.put(ByteArrayComparator.copy(key), Value.put(value, seq));
        if (memtable.approximateBytes() >= config.memtableMaxBytes()) {
            flush();
        }
    }

    @Override
    public synchronized Optional<byte[]> get(byte[] key) throws IOException {
        ensureOpen();

        Optional<Value> inMem = memtable.get(key);
        if (inMem.isPresent()) {
            Value v = inMem.get();
            return v.isTombstone() ? Optional.empty() : Optional.ofNullable(v.valueBytes());
        }

        // Newest SSTable first (L0 style).
        for (SSTableHandle h : sstablesNewestFirst) {
            if (!withinRange(key, h.minKey(), h.maxKey())) continue;
            if (h.bloom() != null && !h.bloom().mightContain(key)) continue;
            try (SSTableReader r = new SimpleSSTableReader(h.sstablePath())) {
                var res = r.lookup(key);
                if (res instanceof lsm.sstable.LookupResult.Found f) return Optional.of(f.value());
                if (res instanceof lsm.sstable.LookupResult.Deleted) return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized void delete(byte[] key) throws IOException {
        ensureOpen();
        long seq = sequence.incrementAndGet();
        long lsn = wal.append(new WALRecord(WALRecord.Type.DELETE, key, null, seq));
        if (options.walSyncPolicy() == LSMOptions.WALSyncPolicy.EVERY_WRITE) {
            try {
                wal.awaitDurable(lsn);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for WAL durability", ie);
            }
        }
        memtable.put(ByteArrayComparator.copy(key), Value.tombstone(seq));
        if (memtable.approximateBytes() >= config.memtableMaxBytes()) {
            flush();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        ensureOpen();
        if (memtable.size() == 0) return;

        long id = System.currentTimeMillis();
        Path out = sstableDir.resolve(String.format("data_%d.sst", id));
        Path bloomPath = bloomPathFor(out);
        SimpleBloomFilter bloom = new SimpleBloomFilter(options.bloomBits(), options.bloomK());
        try (SimpleSSTableWriter w = new SimpleSSTableWriter(out, options.sparseIndexEveryN(), bloom)) {
            w.writeAll(memtable.iterator());
        }
        BloomFilterIO.write(bloomPath, bloom);

        var it = memtable.iterator();
        byte[] minKey = null;
        byte[] maxKey = null;
        while (it.hasNext()) {
            var e = it.next();
            if (minKey == null) minKey = e.getKey();
            maxKey = e.getKey();
        }
        if (minKey == null) minKey = new byte[0];
        if (maxKey == null) maxKey = new byte[0];

        sstablesNewestFirst.add(0, new SSTableHandle(out, bloomPath, minKey, maxKey, bloom));
        memtable = new SkipListMemtable();
        // WAL truncation/rotation will be added when we introduce a manifest/checkpoint.

        maybeTierColdSstables();
        persistManifest();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        compactionThread.interrupt();
        wal.close();
        tieredStorage.close();
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("engine is closed");
    }

    private static Path bloomPathFor(Path sstablePath) {
        return sstablePath.resolveSibling(sstablePath.getFileName().toString().replace(".sst", ".bf"));
    }

    private static boolean withinRange(byte[] key, byte[] minKey, byte[] maxKey) {
        if (minKey == null || maxKey == null || minKey.length == 0 || maxKey.length == 0) return true;
        return ByteArrayComparator.INSTANCE.compare(key, minKey) >= 0 && ByteArrayComparator.INSTANCE.compare(key, maxKey) <= 0;
    }

    private void persistManifest() throws IOException {
        List<Manifest.Entry> entries = new ArrayList<>(sstablesNewestFirst.size());
        for (SSTableHandle h : sstablesNewestFirst) {
            String sstRel = config.dataDir().relativize(h.sstablePath()).toString();
            String bfRel = h.bloomPath() == null ? null : config.dataDir().relativize(h.bloomPath()).toString();
            entries.add(new Manifest.Entry(sstRel, bfRel, h.minKey(), h.maxKey()));
        }
        Manifest.store(config.dataDir(), entries);
    }

    private void compactionLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                TimeUnit.MILLISECONDS.sleep(options.compactionSleepMillis());
                maybeCompact();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (IOException ignored) {
                // best-effort in this learning implementation
            }
        }
    }

    private synchronized void maybeCompact() throws IOException {
        ensureOpen();
        if (sstablesNewestFirst.size() <= options.compactionThresholdSstables()) return;

        // Compact two oldest SSTables into one.
        SSTableHandle a = sstablesNewestFirst.get(sstablesNewestFirst.size() - 1);
        SSTableHandle b = sstablesNewestFirst.get(sstablesNewestFirst.size() - 2);

        Path out = sstableDir.resolve("compact_" + System.currentTimeMillis() + ".sst");
        Path bloomPath = bloomPathFor(out);
        SimpleBloomFilter bloom = new SimpleBloomFilter(options.bloomBits(), options.bloomK());

        try (SimpleSSTableScanner sa = new SimpleSSTableScanner(a.sstablePath());
             SimpleSSTableScanner sb = new SimpleSSTableScanner(b.sstablePath());
             SimpleSSTableWriter w = new SimpleSSTableWriter(out, options.sparseIndexEveryN(), bloom)) {

            Iterator<Map.Entry<byte[], Value>> merged = merged(sa.iterator(), sb.iterator());
            w.writeAll(merged);
        }

        BloomFilterIO.write(bloomPath, bloom);

        // Remove the two inputs (best-effort).
        Files.deleteIfExists(a.sstablePath());
        if (a.bloomPath() != null) Files.deleteIfExists(a.bloomPath());
        Files.deleteIfExists(b.sstablePath());
        if (b.bloomPath() != null) Files.deleteIfExists(b.bloomPath());

        // Replace with the compacted output as the new "oldest".
        sstablesNewestFirst.remove(sstablesNewestFirst.size() - 1);
        sstablesNewestFirst.remove(sstablesNewestFirst.size() - 1);
        sstablesNewestFirst.add(new SSTableHandle(out, bloomPath, new byte[0], new byte[0], bloom));
        maybeTierColdSstables();
        persistManifest();
    }

    private static Iterator<Map.Entry<byte[], Value>> merged(Iterator<SimpleSSTableScanner.Record> a,
                                                             Iterator<SimpleSSTableScanner.Record> b) {
        return new Iterator<>() {
            SimpleSSTableScanner.Record ra = a.hasNext() ? a.next() : null;
            SimpleSSTableScanner.Record rb = b.hasNext() ? b.next() : null;
            Map.Entry<byte[], Value> next;
            boolean loaded = false;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    next = computeNext();
                    loaded = true;
                }
                return next != null;
            }

            @Override
            public Map.Entry<byte[], Value> next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                loaded = false;
                return next;
            }

            private Map.Entry<byte[], Value> computeNext() {
                if (ra == null && rb == null) return null;
                if (ra == null) return takeB();
                if (rb == null) return takeA();
                int cmp = ByteArrayComparator.INSTANCE.compare(ra.key(), rb.key());
                if (cmp < 0) return takeA();
                if (cmp > 0) return takeB();

                // Same key: keep higher sequence.
                Value va = ra.value();
                Value vb = rb.value();
                Map.Entry<byte[], Value> out = Map.entry(
                        ra.key(),
                        (va.sequence() >= vb.sequence()) ? va : vb
                );
                ra = a.hasNext() ? a.next() : null;
                rb = b.hasNext() ? b.next() : null;
                return out;
            }

            private Map.Entry<byte[], Value> takeA() {
                Map.Entry<byte[], Value> out = Map.entry(ra.key(), ra.value());
                ra = a.hasNext() ? a.next() : null;
                return out;
            }

            private Map.Entry<byte[], Value> takeB() {
                Map.Entry<byte[], Value> out = Map.entry(rb.key(), rb.value());
                rb = b.hasNext() ? b.next() : null;
                return out;
            }
        };
    }

    private void maybeTierColdSstables() throws IOException {
        if (!(tieredStorage instanceof LocalDirectoryTieredStorageManager t)) return;

        List<Path> sstPaths = new ArrayList<>(sstablesNewestFirst.size());
        List<Path> bfPaths = new ArrayList<>(sstablesNewestFirst.size());
        for (SSTableHandle h : sstablesNewestFirst) {
            sstPaths.add(h.sstablePath());
            bfPaths.add(h.bloomPath());
        }

        List<LocalDirectoryTieredStorageManager.Move> moves = t.tierMoves(sstPaths, bfPaths);
        if (moves.isEmpty()) return;

        // Update handles for any moved paths.
        for (int i = 0; i < sstablesNewestFirst.size(); i++) {
            SSTableHandle h = sstablesNewestFirst.get(i);
            for (LocalDirectoryTieredStorageManager.Move m : moves) {
                if (h.sstablePath().equals(m.fromSst())) {
                    sstablesNewestFirst.set(i, new SSTableHandle(
                            m.toSst(),
                            (m.toBloom() == null ? h.bloomPath() : m.toBloom()),
                            h.minKey(),
                            h.maxKey(),
                            h.bloom()
                    ));
                    break;
                }
            }
        }
    }
}

