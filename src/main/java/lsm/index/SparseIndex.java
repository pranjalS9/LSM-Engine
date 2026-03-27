package lsm.index;

/**
 *  An SSTable file can be hundreds of MBs. You can't scan it linearly for every read — too slow.
 *  But you also can't keep a full index of every key in memory — too much RAM.
 *  A sparse index is the middle ground — it stores one index entry per block (e.g. every 4KB or every N keys), not per key:
 *
 *   Key          → File Offset
 *   "apple"      → 0
 *   "mango"      → 4096
 *   "strawberry" → 8192
 *   "zebra"      → 12288
 *
 *   To find "orange":
 *   1. Binary search the sparse index → falls between "mango" (4096) and "strawberry" (8192)
 *   2. Seek to offset 4096 in the file
 *   3. Scan that block linearly until "orange" is found or passed
 */
public interface SparseIndex {
    // Placeholder: will hold (key -> offset) entries for SSTable blocks.
}

