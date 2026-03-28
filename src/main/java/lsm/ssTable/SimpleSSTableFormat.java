package lsm.sstable;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * Defines the binary layout constants and footer structure for the SSTable file format
 */
final class SimpleSSTableFormat {
    static final int MAGIC = 0x4C534D31; // "LSM1"

    // On read, the reader checks this value first — if it doesn't match, the file is corrupt, wrong format, or not an SSTable at all
    static final int FOOTER_BYTES = 4 + 8 + 4; // [4 bytes: MAGIC][8 bytes: indexOffset][4 bytes: indexCount]

    private SimpleSSTableFormat() {}

    static ByteBuffer footer(long indexOffset, int indexCount) {
        ByteBuffer b = ByteBuffer.allocate(FOOTER_BYTES).order(BIG_ENDIAN);
        b.putInt(MAGIC);
        b.putLong(indexOffset);
        b.putInt(indexCount);
        b.flip();
        return b;
    }

    //Reading always starts from the footer, works backwards. Writing always goes forward — records first, index after, footer last.
}

