package kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
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

  // every .5 seconds
  private static final int COMPACTION_INTERVAL_MS = 500;

  private boolean shutdown;
  private Thread compactorThread;


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

    compactorThread = new Thread(new Compactor());
    compactorThread.start();
  }

  public void stop() throws InterruptedException {
    log.trace("Stopping database");

    this.shutdown = true;

    for (Segment seg : this.segments.values()) {
      try {
        seg.close();
      } catch (IOException e) {
        log.error("Error closing segment", e);
      }
    }

    log.trace("All segments closed");

    this.compactorThread.join();
  }

  public boolean read(byte[] key, OutputStream out) throws IOException {

    log.trace("read({})", key);

    int segmentId = this.currentSegmentId.get();

    while (segmentId > 0) {
      log.trace("Searching segment with id: {}", segmentId);

      Segment seg = this.segments.get(segmentId);

      if (seg == null || !seg.contains(key)) {
        log.trace("Not found in segment: {}", segmentId);

        if (seg != null && seg.isCompacted()) {
          break;
        }

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

  private List<Path> listSegments() throws IOException {
    return Files.list(Paths.get(this.dbBasePath))
        .filter(Database::isSegmentFileName)
        .sorted(Database::bySegmentId)
        .collect(Collectors.toList());
  }

  private void recover() throws IOException {
    List<Path> paths = listSegments();

    log.trace("Paths to recover: {}", paths);

    for (Path path : paths) {
      Segment seg = recoverPath(path);
      this.currentSegmentId.set(Math.max(seg.getId(), this.currentSegmentId.get()));
      this.segments.put(seg.getId(), seg);
    }

  }

  private static boolean isSegmentFileName(Path path) {
    return Pattern.matches("(seg|compact)(\\d+)?-\\d+\\.bin", path.getFileName().toString());
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

  private static boolean isCompacted(Path path) {
    return path.getFileName().toFile().toString().contains("compact");
  }

  private Segment recoverPath(Path path) throws IOException {
    log.trace("Recovering path: {}", path);

    String pathAsString = path.toString();
    int segmentId = extractSegmentId(path);

    Segment seg = new Segment(pathAsString, null, segmentId, isCompacted(path), 0);

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

    return seg;
  }

  private Segment createOpenSegmentFromPath(Path path) throws IOException {
    String pathAsString = path.toString();
    log.trace("Creating new segment at path: {}", pathAsString);

    File file = path.toFile();

    if (!file.createNewFile()) {
      throw new IOException("Expected new segment to not exist");
    }

    FileOutputStream fileOutputStream = new FileOutputStream(file, true);

    Segment seg = new Segment(pathAsString, fileOutputStream,
        extractSegmentId(path), isCompacted(path), initialSegmentSize);

    return seg;
  }

  private void makeNewSegment() throws IOException {
    try {
      segmentLock.lock();

      int id = this.currentSegmentId.incrementAndGet();
      Path path = Paths.get(this.dbBasePath, String.format("seg-%d.bin", id));
      Segment segment = createOpenSegmentFromPath(path);

      this.segments.put(id, segment);

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

  private class Compactor implements Runnable {
    @Override
    public void run() {
      while (!shutdown) {
        try {
          doCompaction();
          Thread.sleep(COMPACTION_INTERVAL_MS);

        } catch (Exception e) {
          log.error("Error in compaction", e);
        }
      }
    }

    private void doCompaction() throws IOException {
      Map<ByteBuffer, Segment> mostRecentSegment = new HashMap<>();
      int maxSegmentId = 0;

      List<Path> segmentPaths = listSegments();

      segmentPaths = segmentPaths.subList(0, segmentPaths.size() - 1);

      if (segmentPaths.size() <= 1) {
        return;
      }

      log.trace("Beginning compaction of: {}", segmentPaths);

      for (Path segmentPath : segmentPaths) {
        Segment segment = recoverPath(segmentPath);
        maxSegmentId = Math.max(maxSegmentId, segment.getId());

        for (ByteBuffer key : segment.keys()) {
          mostRecentSegment.put(key, segment);
        }
      }

      long timestamp = System.currentTimeMillis();
      Path path = Paths.get(dbBasePath, String.format("compact%d-%d.bin", timestamp, maxSegmentId));
      Segment segment = createOpenSegmentFromPath(path);

      for (Map.Entry<ByteBuffer, Segment> entry : mostRecentSegment.entrySet()) {
        byte[] key = entry.getKey().array();
        Segment original = entry.getValue();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean found = original.read(key, out);

        if (found) {
          ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
          segment.write(key, in);

        } else {
          segment.delete(key);
        }
      }

      segment.close();

      segments.put(segment.getId(), segment);

      int toRemove = segment.getId() - 1;

      while (toRemove > 0) {
        Segment original = segments.get(toRemove);
        segments.remove(toRemove);

        if (original != null) {
          original.deleteFile();
        }

        toRemove--;
      }

      for (Path segmentPath : segmentPaths) {
        segmentPath.toFile().delete();
      }

      log.trace("Compaction of {} done", segmentPaths);
    }
  }

}
