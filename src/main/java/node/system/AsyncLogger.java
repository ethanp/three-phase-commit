package node.system;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Ethan Petuchowski 3/3/15
 */
public class AsyncLogger {
    int a, b;
    public AsyncLogger(int part1, int part2) {
        a = part1;
        b = part2;
    }

    public void OG(String string) {
        System.out.println(String.format(
                "[%d %d %s] %s",
                a,
                b,
                ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT),
                string));
    }
}
