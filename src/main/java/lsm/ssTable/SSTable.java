package lsm.sstable;

/**
 *  An SSTable (Sorted String Table) is an immutable, sorted file on disk produced when the MemTable is flushed.
 *  The id maps to a filename — something like 00001.sst.
 */
public record SSTable(
        long id
) {}

