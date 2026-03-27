package lsm.bloom;

/**
 * A probabilistic data structure that answers: "Is this key definitely not in this SSTable?"
 *  Uses k hash functions, maps each key to k bit positions in a bit array.
 *  - add() sets those bits.
 *  - mightContain() checks if all k bits are set — if any is 0, definitely not present.
 */
public interface BloomFilter {
    /**
     * Called during SSTable writing — every key written to the SSTable is also added to the bloom filter.
     */
    void add(byte[] key);

    /**
     * Called during reads — before doing any disk I/O on an SSTable, check the bloom filter first
     * false: Key is "definitely" not in this SSTable — skip it, zero disk I/O
     * true:
     *      - Key might be in this SSTable — proceed with actual lookup
     *      - It can be false positive
     */
    boolean mightContain(byte[] key);
}

