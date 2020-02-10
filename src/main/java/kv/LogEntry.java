package kv;

public class LogEntry {
  // 8 crc, 1 for tombstone, 4 for key length, and 4 for value length
  // total = 9 + 8 = 17
  private static final int STATIC_HEADER_SIZE = 17;

  private final long crc;
  private final boolean tombstone;
  private final byte[] key;
  private final byte[] value;

  public LogEntry(long crc, boolean tombstone, byte[] key, byte[] value) {
    this.crc = crc;
    this.tombstone = tombstone;
    this.key = key;
    this.value = value;
  }

  public long getCrc() {
    return crc;
  }

  public boolean isTombstone() {
    return tombstone;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  public int size() {
    return STATIC_HEADER_SIZE + key.length + value.length;
  }
}
