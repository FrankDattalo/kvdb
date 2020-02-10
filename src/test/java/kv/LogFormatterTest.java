package kv;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class LogFormatterTest {

  private static byte[] writeToByteArray(String key, String value) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    LogFormatter.write(out,
        new ByteArrayInputStream(key.getBytes()),
        new ByteArrayInputStream(value.getBytes()));

    return out.toByteArray();
  }

  @Test
  public void testWrite() throws IOException {

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteArrayOutputStream);

    out.write(new byte[]{0, 0, 0, 0, 34, -109, 43, -78});
    out.writeInt(5);
    out.writeInt(5);
    out.writeBoolean(false);
    out.write("hello".getBytes());
    out.write("world".getBytes());
    out.flush();

    byte[] byteArray = writeToByteArray("hello", "world");

    assertArrayEquals(byteArrayOutputStream.toByteArray(), byteArray);
  }

  private void testCorrupted(Consumer<byte[]> withByteArray) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    buffer.write(writeToByteArray("some key", "some value"));
    buffer.flush();

    byte[] bytes = buffer.toByteArray();

    withByteArray.accept(bytes);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);

    try {
      LogFormatter.read(in, new ByteArrayOutputStream());
      Assert.fail();

    } catch (IOException e) {
      String message = e.getMessage().toLowerCase();
      assertEquals(message, "crc mismatch");
    }
  }

  @Test
  public void testReadWithCorruptionInCrc() throws IOException {
    testCorrupted(arr -> {
      for (int i = 0; i < 8; i++) {
        arr[i] = 0;
      }
    });
  }

  @Test
  public void testReadWithCorruptionInUnfinishedLog() throws IOException {
    testCorrupted(arr -> {
      arr[arr.length - 1] = 0;
    });
  }
}
