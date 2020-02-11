package kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Segment {

  private static final Logger log = LoggerFactory.getLogger(Segment.class);

  private final Map<ByteSlice, Long> offsets = Collections.synchronizedMap(new HashMap<>());
  private final String filePath;
  private final FileOutputStream fileOutputStream;
  private final int id;
  private final boolean compacted;
  private final long resizeAtBytes;
  private final ReentrantLock writeLock = new ReentrantLock();

  public Segment(
      String filePath, FileOutputStream fileOutputStream,
      int id, boolean compacted, long resizeAtBytes) {

    this.filePath = filePath;
    this.fileOutputStream = fileOutputStream;
    this.id = id;
    this.compacted = compacted;
    this.resizeAtBytes = resizeAtBytes;
  }

  public Iterable<ByteSlice> keys() {
    return this.offsets.keySet();
  }

  public void put(ByteSlice key, long offset) {
    offsets.put(key, offset);
    log.trace("Updating offset of {} to {}", key, offset);
  }

  public void write(ByteSlice key, InputStream value) throws IOException {

    checkWrite();

    try {
      writeLock.lock();

      long currentOffset = fileOutputStream.getChannel().position();
      LogFormatter.write(fileOutputStream, key.toStream(), value);
      put(key, currentOffset);

    } finally {
      writeLock.unlock();
    }
  }

  public boolean read(ByteSlice key, OutputStream out) throws IOException {
    long position = this.offsets.get(key);

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.filePath, "r")) {

      log.trace("Seeking to offset of {} for {}", key, position);

      randomAccessFile.seek(position);

      try (InputStream data = Channels.newInputStream(randomAccessFile.getChannel())) {

        log.trace("Reading data at offset");

        return LogFormatter.read(data, out);
      }
    }
  }

  public void delete(ByteSlice key) throws IOException {
    write(key, null);
  }

  public void deleteFile() throws IOException {
    log.trace("deleteFile()");

    this.close();

    File file = new File(filePath);

    if (!file.delete()) {
      throw new IOException("Could not delete segment: " + filePath);
    }
  }

  public boolean contains(ByteSlice key) {
    return offsets.containsKey(key);
  }

  public boolean isAtCapacity() throws IOException {
    log.trace("Checking capacity of segment");

    checkWrite();

    return this.fileOutputStream.getChannel().size() >= this.resizeAtBytes;
  }

  public int getId() {
    return this.id;
  }

  public boolean isCompacted() {
    return compacted;
  }

  public void close() throws IOException {

    log.trace("Closing segment");

    if (this.fileOutputStream != null) {
      this.fileOutputStream.close();
    }
  }

  private void checkWrite() throws IOException {
    if (fileOutputStream == null) {
      throw new IOException("File is closed, cannot write");
    }
  }

  @Override
  public String toString() {
    return this.offsets.entrySet()
        .stream().map(this::entryToString)
        .collect(Collectors.toList()).toString();
  }

  private String entryToString(Map.Entry<ByteSlice, Long> entry) {
    try {
      ByteSlice key = entry.getKey();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      boolean found = this.read(key, out);

      return String.format("%s => %s",
          new String(key.copyData(), StandardCharsets.UTF_8),
          found ? new String(out.toByteArray(), StandardCharsets.UTF_8) : "<tombstone>");

    } catch (IOException e) {
      return "IOException";
    }
  }
}
