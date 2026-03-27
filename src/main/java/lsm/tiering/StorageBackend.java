package lsm.tiering;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StorageBackend {

    //Get a stream to read an existing file (SSTable, WAL)
    InputStream openForRead(String path) throws IOException;

    //Get a stream to write a new file
    OutputStream openForWrite(String path) throws IOException;

    //Check if a file is already there (used during recovery, compaction)
    boolean exists(String path) throws IOException;

    //Atomic rename — critical for safe SSTable replacement during compaction
    void move(String from, String to) throws IOException;
}

