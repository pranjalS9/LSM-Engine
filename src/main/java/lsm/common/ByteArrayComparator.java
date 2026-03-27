package lsm.common;

import java.util.Arrays;
import java.util.Comparator;

//Defines how keys are ordered in the LSM Engine.
// Final class prevents sub-classing
public final class ByteArrayComparator implements Comparator<byte[]> {
    //One instance shared everywhere — no allocation overhead, no risk of different comparators producing different orderings
    public static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

    private ByteArrayComparator() {}

    //lexicographic byte ordering ("foo" vs "foobar" — the shorter one sorts first)
    @Override
    public int compare(byte[] a, byte[] b) {
        if(a == b) return 0;
        if(a == null) return -1; //a is smaller
        if(b == null) return 1; //a is bigger

        int min = Math.min(a.length, b.length);

        /*
        - Loops through both arrays up to the shorter length
        - (& 0xFF) -> converts signed byte (-128 to 127) to unsigned int (0 to 255) — to compare bytes correctly
        - If a difference is found, return which one is bigger
         */
        for (int i = 0; i < min; i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return Integer.compare(ai, bi);
        }

        //If all compared bytes are equal, the shorter array is smaller
        return Integer.compare(a.length, b.length);
    }

    /*
     - Keys are passed around a lot between different components.
     - If we don't copy them, one component could accidentally modify a key that another component is still using,
       causing silent data corruption. copy() prevents that.
     */
    public static byte[] copy(byte[] src) {
        return src == null ? null : Arrays.copyOf(src, src.length);
    }

}
