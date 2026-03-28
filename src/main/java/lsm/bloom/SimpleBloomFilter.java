package lsm.bloom;
import java.util.BitSet;
public final class SimpleBloomFilter implements BloomFilter {
    private final int bits;
    private final int k;
    private final BitSet bitset;
    public SimpleBloomFilter(int bits, int k) {
        if (bits <= 0) throw new IllegalArgumentException("bits must be > 0");
        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
        this.bits = bits;
        this.k = k;
        this.bitset = new BitSet(bits);
    }
    public int bits() {
        return bits;
    }
    public int k() {
        return k;
    }
    public byte[] toByteArray() {
        return bitset.toByteArray();
    }
    public static SimpleBloomFilter fromBytes(int bits, int k, byte[] bytes) {
        SimpleBloomFilter bf = new SimpleBloomFilter(bits, k);
        BitSet bs = BitSet.valueOf(bytes);
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            bf.bitset.set(i);
        }
        return bf;
    }
    @Override
    public void add(byte[] key) {
        long h1 = murmur3_32(key, 0x9747b28c) & 0xffffffffL;
        long h2 = murmur3_32(key, 0x3c074a61) & 0xffffffffL;
        for (int i = 0; i < k; i++) {
            long combined = h1 + (long) i * h2;
            int idx = (int) (Math.floorMod(combined, bits));
            bitset.set(idx);
        }
    }
    @Override
    public boolean mightContain(byte[] key) {
        long h1 = murmur3_32(key, 0x9747b28c) & 0xffffffffL;
        long h2 = murmur3_32(key, 0x3c074a61) & 0xffffffffL;
        for (int i = 0; i < k; i++) {
            long combined = h1 + (long) i * h2;
            int idx = (int) (Math.floorMod(combined, bits));
            if (!bitset.get(idx)) return false;
        }
        return true;
    }
    // Minimal Murmur3 x86 32-bit
    private static int murmur3_32(byte[] data, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = seed;
        int len = data.length;
        int roundedEnd = (len & 0xfffffffc);
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        int tail = len & 0x03;
        if (tail == 3) k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
        if (tail >= 2) k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
        if (tail >= 1) {
            k1 ^= (data[roundedEnd] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }
        h1 ^= len;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }
}
