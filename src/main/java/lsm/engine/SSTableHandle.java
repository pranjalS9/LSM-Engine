package lsm.engine;

import lsm.bloom.SimpleBloomFilter;

import java.nio.file.Path;

record SSTableHandle(
        Path sstablePath,
        Path bloomPath,
        byte[] minKey,
        byte[] maxKey,
        SimpleBloomFilter bloom
) {}

