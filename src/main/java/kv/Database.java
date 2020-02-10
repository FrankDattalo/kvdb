package kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Database {

  private static final Logger log = LoggerFactory.getLogger(Database.class);

  private final AtomicInteger currentSegmentId = new AtomicInteger(0);
  private final Map<Integer, Segment> segments = Collections.synchronizedMap(new HashMap<>());
  private final ReentrantLock segmentLock = new ReentrantLock();
  private final String dbBasePath;
  private final long initialSegmentSize;

  // ~100 mb, this should never really be hit
  private static final int MAX_READ_LIMIT = 1000 * 1000 * 100;

  public Database(String dbBasePath, long initialSegmentSize) {
    this.dbBasePath = dbBasePath;
    this.initialSegmentSize = initialSegmentSize;
  }

  public void start() throws IOException {

    log.trace("Begin startup process");

    if (!Files.exists(Paths.get(this.dbBasePath))) {
      log.trace("Db base path does not exist, running first time setup");

      initialSetup();

    } else {
      log.trace("Db base path does exist, assuming recovery from previous execution");

      recover();
    }

    this.makeNewSegment();
  }

  public void stop() {
    log.trace("Stopping database");

    for (Segment seg : this.segments.values()) {
      try {
        seg.close();
      } catch (IOException e) {
        log.error("Error closing segment", e);
      }
    }

    log.trace("All segments closed");
  }

  public boolean read(byte[] key, OutputStream out) throws IOException {

    log.trace("read({})", key);

    int segmentId = this.currentSegmentId.get();

    while (segmentId > 0) {
      log.trace("Searching segment with id: {}", segmentId);

      Segment seg = this.segments.get(segmentId);

      if (seg == null || !seg.contains(key)) {
        log.trace("Not found in segment: {}", segmentId);

        segmentId--;
        continue;
      }

      return seg.read(key, out);
    }

    log.trace("read({}) - not found", key);

    return false;
  }

  public void write(byte[] key, InputStream value) throws IOException {

    log.trace("write({})", key);

    Segment seg = this.currentSegment();
    seg.write(key, value);
    this.checkSegment();
  }

  public void delete(byte[] key) throws IOException {
    log.trace("delete({})", key);

    Segment seg = this.currentSegment();
    seg.delete(key);
    this.checkSegment();
  }

  private void initialSetup() throws IOException {

    File file = new File(this.dbBasePath);

    if (!file.mkdir()) {
      throw new IOException("Could not create base directory");
    }

  }

  private void recover() throws IOException {
    List<Path> paths = Files.list(Paths.get(this.dbBasePath))
        .filter(Database::isSegmentFileName)
        .sorted(Database::bySegmentId)
        .collect(Collectors.toList());

    log.trace("Paths to recover: {}", paths);

    for (Path path : paths) {
      recoverPath(path);
    }

  }

  private static boolean isSegmentFileName(Path path) {
    return Pattern.matches("seg-\\d+\\.bin", path.getFileName().toString());
  }

  private static int bySegmentId(Path path1, Path path2) {
    return extractSegmentId(path1) - extractSegmentId(path2);
  }

  private static int extractSegmentId(Path path) {
    String fileName = path.getFileName().toString();
    int slashIndex = fileName.indexOf('-');
    int dotIndex = fileName.indexOf('.');
    return Integer.parseInt(fileName.substring(slashIndex + 1, dotIndex));
  }

  private void recoverPath(Path path) throws IOException {
    log.trace("Recovering path: {}", path);

    String pathAsString = path.toString();
    int segmentId = extractSegmentId(path);

    this.currentSegmentId.set(Math.max(segmentId, this.currentSegmentId.get()));

    Segment seg = new Segment(pathAsString, null, 0);

    this.segments.put(segmentId, seg);

    File file = path.toFile();

    try (FileInputStream in = new FileInputStream(file)) {
      BufferedInputStream buffered = new BufferedInputStream(in);

      long currentOffset = 0;

      while (true) {
        buffered.mark(MAX_READ_LIMIT);

        try {
          LogEntry entry = LogFormatter.readLogEntry(buffered);
          seg.put(entry.getKey(), currentOffset);
          currentOffset += entry.size();

        } catch (IOException e) {
          if (!(e instanceof EOFException)) {
            log.trace("Got exception while trying to recover entry, advancing one byte and trying again", e);
          }

          buffered.reset();

          int byteRead = buffered.read();
          currentOffset++;

          if (byteRead < 0) {
            break;
          }
        }
      }
    }
  }

  private void makeNewSegment() throws IOException {
    try {
      segmentLock.lock();

      int id = this.currentSegmentId.incrementAndGet();
      Path path = Paths.get(this.dbBasePath, String.format("seg-%d.bin", id));
      String pathAsString = path.toString();

      log.trace("Creating new segment at path: {}", pathAsString);

      File file = path.toFile();

      if (!file.createNewFile()) {
        throw new IOException("Expected new segment to not exist");
      }

      FileOutputStream fileOutputStream = new FileOutputStream(file, true);
      Segment seg = new Segment(pathAsString, fileOutputStream, initialSegmentSize);
      this.segments.put(id, seg);

    } finally {
      segmentLock.unlock();
    }
  }

  private void checkSegment() throws IOException {
    try {
      segmentLock.lock();

      Segment segment = currentSegment();

      if (segment.isAtCapacity()) {
        log.trace("Current segment is at capacity, new segment will be created");
        segment.close();
        this.makeNewSegment();
      }

    } finally {
      segmentLock.unlock();
    }
  }

  private Segment currentSegment() {
    try {
      segmentLock.lock();

      return this.segments.get(currentSegmentId.get());

    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  public String toString() {
    return this.segments.toString();
  }
}
