package lsm.sstable;

import lsm.common.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChecksumTest {

    @Test
    void corruptedRecordThrowsOnLookup() throws Exception {
        Path dir = Files.createTempDirectory("lsm-checksum");
        Path sstPath = dir.resolve("test.sst");

        // Write a single record directly via SimpleSSTableWriter
        Value val = Value.put(b("value1"), 1L);
        try (SimpleSSTableWriter w = new SimpleSSTableWriter(sstPath, 32, null)) {
            w.writeAll(Map.of(b("key1"), val).entrySet().iterator());
        }

        // Record layout: keyLen(4) + valLen(4) + seq(8) + key(4) + value(6) + crc(4)
        // Value starts at offset 16 + 4 = 20. Corrupt first byte of value.
        try (FileChannel ch = FileChannel.open(sstPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            ch.read(buf, 20);
            buf.flip();
            buf.put(0, (byte) (buf.get(0) ^ 0xFF));
            // buf is already flipped (position=0, limit=1) — write directly without flipping again
            ch.write(buf, 20);
        }

        // Verify the corruption actually happened
        try (FileChannel verifyChannel = FileChannel.open(sstPath, StandardOpenOption.READ)) {
            ByteBuffer verifyBuf = ByteBuffer.allocate(1);
            verifyChannel.read(verifyBuf, 20);
            verifyBuf.flip();
            assertEquals((byte)('v' ^ 0xFF), verifyBuf.get(0), "Byte 20 should be corrupted");
        }

        // Lookup must throw IOException due to checksum mismatch
        try (SimpleSSTableReader reader = new SimpleSSTableReader(sstPath)) {
            LookupResult result = reader.lookup(b("key1"));
            fail("Expected IOException but got result: " + result);
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("checksum"), "Expected checksum error but got: " + e.getMessage());
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}