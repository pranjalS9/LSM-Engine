package lsm.sstable;

public sealed interface LookupResult permits LookupResult.Found, LookupResult.Deleted, LookupResult.NotFound {
    record Found(byte[] value) implements LookupResult {}
    record Deleted() implements LookupResult {}
    record NotFound() implements LookupResult {}
}

