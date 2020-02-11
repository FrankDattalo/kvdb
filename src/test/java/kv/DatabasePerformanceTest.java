package kv;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(JUnit4.class)
public class DatabasePerformanceTest {

  private void runMixedPerformanceTest(
      String dbPath,
      long segmentSize,
      int threadCount,
      long operationsPerThread,
      float readPercent,
      float writePercent,
      float deletePercent) throws IOException, InterruptedException {

    Database database = new Database(dbPath, segmentSize);

    database.start();

    List<PerformanceTestThread> threads = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      threads.add(new PerformanceTestThread(
          database,
          operationsPerThread,
          readPercent,
          writePercent,
          deletePercent));
    }

    long startTime = System.currentTimeMillis();

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    long stopTime = System.currentTimeMillis();

    long elapsed = stopTime - startTime;

    long totalReads = 0;
    long totalWrites = 0;
    long totalDeletes = 0;

    for (PerformanceTestThread thread : threads) {
      totalReads += thread.reads;
      totalWrites += thread.writes;
      totalDeletes += thread.deletes;
    }

    long total = totalReads + totalWrites + totalDeletes;

    database.stop();

    display("Elapsed", "%.2fs", elapsed / 1000.0);
    display("Threads", "%s", threadCount);
    display("Ops Per Thread", "%s", operationsPerThread);
    display("Total Reads", "%s/%s (%.2f%%)", totalReads, total, ((double) totalReads) / total);
    display("Total Writes", "%s/%s (%.2f%%)", totalWrites, total, ((double) totalWrites) / total);
    display("Total Deletes", "%s/%s (%.2f%%)", totalDeletes, total, ((double) totalDeletes) / total);
    display("Reads/Second", "%.2f", perSecond(totalReads, elapsed));
    display("Writes/Second", "%.2f", perSecond(totalWrites, elapsed));
    display("Deletes/Second", "%.2f", perSecond(totalDeletes, elapsed));

    FileUtils.deleteDirectory(new File(dbPath));
  }

  private static void display(String description, String format, Object... args) {
    System.out.printf("%20s:%30s\n", description, String.format(format, args));
  }

  private double perSecond(long total, long elapsedMillis) {
    double totalPerMilli = ((double) total) / elapsedMillis;
    return totalPerMilli * 1000;
  }

  @Test
  public void test1() throws IOException, InterruptedException {
    runMixedPerformanceTest(
        randomPath(),
        1000,
        100,
        1000,
        0.8f,
        0.1f,
        0.1f);
  }

  private static class PerformanceTestThread extends Thread {

    private final Database database;
    private final long operationsPerThread;
    private final float readPercent;
    private final float writePercent;
    private final float deletePercent;

    private long reads;
    private long writes;
    private long deletes;

    public PerformanceTestThread(
        Database database,
        long operationsPerThread,
        float readPercent, float writePercent, float deletePercent) {

      this.database = database;
      this.operationsPerThread = operationsPerThread;
      this.readPercent = readPercent;
      this.writePercent = writePercent;
      this.deletePercent = deletePercent;
    }

    @Override
    public void run() {

      float totalPercent = readPercent + writePercent + deletePercent;
      float readPercent = this.readPercent / totalPercent;
      float writePercent = this.writePercent / totalPercent;

      float readLower = 0;
      float readUpper = readPercent;

      float writeLower = readUpper;
      float writeUpper = writeLower + writePercent;

      float deleteLower = writeUpper;
      float deleteUpper = 1;

      for (long i = 0; i < operationsPerThread; i++) {
        try {
          float op = (float) Math.random();

          if (readLower <= op && op < readUpper) {
            reads++;
            this.database.read(randomId(), new ByteArrayOutputStream());

          } else if (writeLower <= op && op < writeUpper) {
            writes++;
            this.database.write(randomId(), randomId().toStream());

          } else if (deleteLower <= op && op < deleteUpper) {
            deletes++;
            this.database.delete(randomId());
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static ByteSlice randomId() {
    byte randomByte = (byte) (Math.random() * 256);
    return new ByteSlice(new byte[]{randomByte});
  }

  private static String randomPath() {
    return "./" + UUID.randomUUID().toString();
  }
}
