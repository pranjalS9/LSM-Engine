package lsm.bloom;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import static java.nio.ByteOrder.BIG_ENDIAN;
public final class BloomFilterIO {
    private static final int MAGIC = 0x424C4F4D; // "BLOM"
    private BloomFilterIO() {}
    public static void write(Path path, SimpleBloomFilter bf) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        byte[] bytes = bf.toByteArray();
        ByteBuffer header = ByteBuffer.allocate(4 + 4 + 4 + 4).order(BIG_ENDIAN);
        header.putInt(MAGIC);
        header.putInt(bf.bits());
        header.putInt(bf.k());
        header.putInt(bytes.length);
        header.flip();
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            writeFully(ch, header);
            writeFully(ch, ByteBuffer.wrap(bytes));
            ch.force(true);
        }
    }
    public static SimpleBloomFilter read(Path path) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(4 + 4 + 4 + 4).order(BIG_ENDIAN);
            readFully(ch, header, 0);
            header.flip();
            int magic = header.getInt();
            if (magic != MAGIC) throw new IOException("invalid bloom magic");
            int bits = header.getInt();
            int k = header.getInt();
            int len = header.getInt();
            if (len < 0 || len > 512 * 1024 * 1024) throw new IOException("invalid bloom length");
            byte[] bytes = new byte[len];
            readFully(ch, ByteBuffer.wrap(bytes), header.capacity());
            return SimpleBloomFilter.fromBytes(bits, k, bytes);
        }
    }
    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }
    private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        while (dst.hasRemaining()) {
            int n = ch.read(dst, position);
            if (n < 0) throw new EOFException();
            position += n;
        }
    }
}
