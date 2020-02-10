package kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Segment {

  private static final Logger log = LoggerFactory.getLogger(Segment.class);

  private final Map<ByteBuffer, Long> offsets = Collections.synchronizedMap(new HashMap<>());
  private final String filePath;
  private final FileOutputStream fileOutputStream;
  private final long resizeAtBytes;

  public Segment(String filePath, FileOutputStream fileOutputStream, long resizeAtBytes) {
    this.filePath = filePath;
    this.fileOutputStream = fileOutputStream;
    this.resizeAtBytes = resizeAtBytes;
  }

  public void put(byte[] key, long offset) {
    offsets.put(ByteBuffer.wrap(key), offset);
    log.trace("Updating offset of {} to {}", key, offset);
  }

  public void write(byte[] key, InputStream value) throws IOException {

    checkWrite();

    long currentOffset = fileOutputStream.getChannel().position();
    LogFormatter.write(fileOutputStream, new ByteArrayInputStream(key), value);
    put(key, currentOffset);
  }

  public boolean read(byte[] key, OutputStream out) throws IOException {
    long position = this.offsets.get(ByteBuffer.wrap(key));

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.filePath, "r")) {

      log.trace("Seeking to offset of {} for {}", key, position);

      randomAccessFile.seek(position);

      try (InputStream data = Channels.newInputStream(randomAccessFile.getChannel())) {

        log.trace("Reading data at offset");

        return LogFormatter.read(data, out);
      }
    }
  }

  public void delete(byte[] key) throws IOException {
    write(key, null);
  }

  public boolean contains(byte[] key) {
    return offsets.containsKey(ByteBuffer.wrap(key));
  }

  public boolean isAtCapacity() throws IOException {
    log.trace("Checking capacity of segment");

    checkWrite();

    return this.fileOutputStream.getChannel().size() >= this.resizeAtBytes;
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
        .stream().map(Segment::entryToString)
        .collect(Collectors.toList()).toString();
  }

  private static String entryToString(Map.Entry<ByteBuffer, Long> entry) {
    return String.format("%s => %d",
        Arrays.toString(entry.getKey().array()),
        entry.getValue());
  }
}
