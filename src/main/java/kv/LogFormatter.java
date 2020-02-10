package kv;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.CRC32;

public class LogFormatter {

  private static final Logger log = LoggerFactory.getLogger(LogFormatter.class);

  public static class CrcMismatchException extends IOException {
    public CrcMismatchException() {
      super("Crc mismatch");
    }
  }

  public static long crc(byte[] array) {
    CRC32 crc32 = new CRC32();
    crc32.update(array);
    return crc32.getValue();
  }

  public static LogEntry readLogEntry(InputStream in) throws IOException {

    log.trace("readLogEntry()");

    final DataInputStream inputStream = new DataInputStream(in);
    final ByteArrayOutputStream crcBuffer = new ByteArrayOutputStream();
    final DataOutputStream crcBufferDataOutputStream = new DataOutputStream(crcBuffer);

    final long crc = inputStream.readLong();

    final int keyBytesLength = inputStream.readInt();
    final int valueBytesLength = inputStream.readInt();
    final boolean tombstone = inputStream.readBoolean();

    final byte[] keyBytes = new byte[keyBytesLength];

    if (in.read(keyBytes) != keyBytesLength) {
      throw new IOException("Unexpected EOF when reading key");
    }

    final byte[] valueBytes = new byte[valueBytesLength];

    if (in.read(valueBytes) != valueBytesLength) {
      throw new IOException("Unexpected EOF when reading value");
    }

    writeLogEntry(crcBufferDataOutputStream, tombstone, keyBytes, valueBytes);

    final byte[] allBytes = crcBuffer.toByteArray();

    if (crc(allBytes) != crc) {
      throw new CrcMismatchException();
    }

    return new LogEntry(crc, true, keyBytes, valueBytes);
  }

  public static boolean read(InputStream in, OutputStream out) throws IOException {

    log.trace("read()");

    LogEntry logEntry = readLogEntry(in);

    if (logEntry.isTombstone()) {
      return false;
    }

    if (out != null) {
      out.write(logEntry.getValue());
      out.flush();
    }

    return true;
  }

  private static void writeLogEntry(
      DataOutputStream out, boolean tombstone,
      byte[] keyBytes, byte[] valueBytes) throws IOException {

    log.trace("writeLogEntry()");

    out.writeInt(keyBytes.length);
    out.writeInt(tombstone ? 0 : valueBytes.length);
    out.writeBoolean(tombstone);
    out.write(keyBytes);

    if (!tombstone) {
      out.write(valueBytes);
    }

    out.flush();
  }

  public static void write(
      final OutputStream out,
      final InputStream key,
      final InputStream value) throws IOException {

    log.trace("write()");

    final ByteArrayOutputStream keyBuffer = new ByteArrayOutputStream();
    final ByteArrayOutputStream valueBuffer = new ByteArrayOutputStream();
    final ByteArrayOutputStream joinedBuffer = new ByteArrayOutputStream();

    final DataOutputStream joinedBufferDataOutputStream = new DataOutputStream(joinedBuffer);
    final DataOutputStream totalBufferDataOutputStream = new DataOutputStream(out);

    IOUtils.copy(key, keyBuffer);

    if (value != null) {
      IOUtils.copy(value, valueBuffer);
    }

    writeLogEntry(joinedBufferDataOutputStream,
        value == null, keyBuffer.toByteArray(), valueBuffer.toByteArray());

    final byte[] allBytes = joinedBuffer.toByteArray();

    totalBufferDataOutputStream.writeLong(crc(allBytes));
    joinedBuffer.writeTo(totalBufferDataOutputStream);
    totalBufferDataOutputStream.flush();

  }
}
