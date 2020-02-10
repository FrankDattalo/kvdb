package kv;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class App {

  private static final Logger log = LoggerFactory.getLogger(App.class);

  private static void help() {
    System.out.println("Commands: ");
    System.out.println("  /help - prints this command");
    System.out.println("  /read <key> - reads the given key");
    System.out.println("  /write <key> <value> - inserts / updates the given key");
    System.out.println("  /delete <key> - deletes the given key");
    System.out.println("  /debug - prints the database contents");
    System.out.println("  /quit - quits the program");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Scanner scanner = new Scanner(System.in);

    Database database = null;

    if (args.length > 2 && args[2].equals("true")) {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      Configuration config = context.getConfiguration();
      LoggerConfig rootConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
      rootConfig.setLevel(Level.TRACE);
      context.updateLoggers();
    }

    try {
      log.trace("Initializing database");

      database = new Database(args[0], Long.parseLong(args[1]));
      database.start();

      log.trace("Database initialized");

      help();

      while (true) {
        System.out.print("> ");

        try {
          String line = scanner.nextLine();

          if (line == null || line.startsWith("/quit")) {
            break;
          }

          if (line.startsWith("/read")) {
            String key = line.substring("/read".length()).trim();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            boolean found = database.read(new ByteSlice(key.getBytes()), out);
            System.out.println(found ? new String(out.toByteArray()) : "<Not Found>");

          } else if (line.startsWith("/write")) {
            String rest = line.substring("/write".length()).trim();
            int firstSpace = rest.indexOf(' ');
            String key = rest.substring(0, firstSpace).trim();
            String value = rest.substring(firstSpace).trim();
            database.write(new ByteSlice(key.getBytes()),
                new ByteArrayInputStream(value.getBytes()));

          } else if (line.startsWith("/delete")) {
            String key = line.substring("/delete".length()).trim();
            database.delete(new ByteSlice(key.getBytes()));

          } else if (line.startsWith("/help")) {
            help();

          } else if (line.startsWith("/debug")) {
            System.out.println(database);

          } else {
            System.out.println("Invalid command: " + line);
            help();
          }

        } catch (Exception e) {
          if (e instanceof NoSuchElementException) {
            return;
          } else {
            e.printStackTrace();
          }
        }
      }

    } finally {
      if (database != null) database.stop();
    }
  }
}
