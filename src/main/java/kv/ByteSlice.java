package kv;

import java.io.InputStream;
import java.util.Arrays;

public class ByteSlice {

  private final byte[] data;

  public ByteSlice(byte[] data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ByteSlice byteSlice = (ByteSlice) o;
    return Arrays.equals(data, byteSlice.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }

  public byte[] copyData() {
    return Arrays.copyOf(data, data.length);
  }

  public InputStream toStream() {
    return new InputStream() {
      int position = 0;

      @Override
      public int read() {
        if (position >= data.length) return -1;

        int byte_ = data[position];

        position++;

        return byte_;
      }
    };
  }

  @Override
  public String toString() {
    return Arrays.toString(data);
  }
}
