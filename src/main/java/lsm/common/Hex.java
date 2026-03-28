package lsm.common;

/**
 * A utility for converting between byte[] and hex strings.
 * Used for debugging, logging, and serializing binary keys/values in human-readable form.
 *
 * Without Hex, binary keys would be unreadable in logs and files.
 * Storing them as hex in the manifest avoids encoding issues with arbitrary bytes
 */
public final class Hex {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Hex() {}

    public static String toHex(byte[] bytes) {
        if (bytes == null) return "";
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2] = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    public static byte[] fromHex(String hex) {
        if (hex == null || hex.isEmpty()) return new byte[0];
        if ((hex.length() & 1) == 1) throw new IllegalArgumentException("hex length must be even");
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            int hi = Character.digit(hex.charAt(i * 2), 16);
            int lo = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) throw new IllegalArgumentException("invalid hex");
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }
}

