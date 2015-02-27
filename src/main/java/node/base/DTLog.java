package node.base;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class DTLog {

    protected Writer writer;
    Node node;

    protected DTLog(Node node) {
        this.node = node;
    }

    DTLog(Node node, File file) {
        this.node = node;
        try {
            writer = new FileWriter(file);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    void log(String string) {
        try {
            /* SAMPLE LOG
            [3 2:32:34.3545] 2 ADD song_1 url_1
            [3 2:32:34.3545] 5 SENT Yes vote
            ...

            */
            String logOutput = String.format(
                    "[%d %s] %s",
                    node.getMyNodeID(),
                    ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT),
                    string);

            writer.write(logOutput);
            writer.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public abstract String getLogAsString();

}
