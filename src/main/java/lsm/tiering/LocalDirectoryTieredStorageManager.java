package lsm.tiering;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public final class LocalDirectoryTieredStorageManager implements TieredStorageManager {
    public record Move(Path fromSst, Path toSst, Path fromBloom, Path toBloom) {}

    private final Path remoteDir;
    private final int keepLocalSstables;
    private volatile boolean closed = false;

    public LocalDirectoryTieredStorageManager(Path remoteDir, int keepLocalSstables) throws IOException {
        this.remoteDir = remoteDir;
        this.keepLocalSstables = Math.max(0, keepLocalSstables);
        Files.createDirectories(remoteDir);
    }

    @Override
    public void maybeTierColdData() {
        // no-op: call tierMoves(...) explicitly with the current state
    }

    public List<Move> tierMoves(List<Path> sstablesNewestFirst, List<Path> bloomsNewestFirst) throws IOException {
        ensureOpen();
        if (sstablesNewestFirst.size() <= keepLocalSstables) return List.of();

        List<Move> moves = new ArrayList<>();
        for (int i = keepLocalSstables; i < sstablesNewestFirst.size(); i++) {
            Path fromSst = sstablesNewestFirst.get(i);
            Path toSst = remoteDir.resolve(fromSst.getFileName().toString());
            Path fromBloom = (bloomsNewestFirst != null && i < bloomsNewestFirst.size()) ? bloomsNewestFirst.get(i) : null;
            Path toBloom = (fromBloom == null) ? null : remoteDir.resolve(fromBloom.getFileName().toString());

            Files.createDirectories(remoteDir);
            moveBestEffort(fromSst, toSst);
            if (fromBloom != null && toBloom != null) {
                moveBestEffort(fromBloom, toBloom);
            }
            moves.add(new Move(fromSst, toSst, fromBloom, toBloom));
        }
        return moves;
    }

    @Override
    public void close() {
        closed = true;
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("tiered storage manager is closed");
    }

    private static void moveBestEffort(Path from, Path to) throws IOException {
        if (!Files.exists(from)) return;
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {
            // If atomic move is unsupported across devices, fallback.
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}

