package lsm;

import lsm.engine.EngineConfig;
import lsm.engine.InMemoryStorageEngine;
import lsm.engine.StorageEngine;

import java.nio.file.Path;
import java.util.Optional;

public class Main {
    public static void main(String[] args) throws Exception {
        /*
            1. Create config — memtable max 4MB, data dir = ./data
            2. Create InMemoryStorageEngine with that config
            3. engine.put("hello", "world")
                 - sequence increments to 1
                 - Value.put("world".getBytes(), seq=1) created
                 - SkipListMemtable stores: "hello" → Value(world, seq=1)
            4. engine.get("hello")
                 - SkipListMemtable.get("hello") → Optional<Value>
                 - Value is not a tombstone → return valueBytes()
                 - Result: Optional<byte[]> containing "world" bytes
            5. got.map(String::new) → "world"
                .orElse("<missing>") → "world"
                System.out.println → prints "world"
            6. try-with-resources exits → engine.close() called
               - closed = true
         */
        EngineConfig cfg = new EngineConfig(Path.of("data"), 4 * 1024 * 1024);
        try (StorageEngine engine = new InMemoryStorageEngine(cfg)) {
            engine.put("hello".getBytes(), "world".getBytes());
            Optional<byte[]> got = engine.get("hello".getBytes());
            System.out.println(got.map(String::new).orElse("<missing>"));
        }
    }
}

