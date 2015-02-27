package system.node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Ethan Petuchowski 2/26/15
 */
public class DTLog {
    File file;
    FileWriter writer;
    Node node;
    DTLog(Node node, File file) {
        this.node = node;
        this.file = file;
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
            [3 2:32:34.3545 3/4/15] ADD song_1 url_1
            [3 2:32:34.3545 3/4/15] SENT Yes vote
            ...

            */
            writer.write(node.getMyNodeID() +LocalTime.now().format(DateTimeFormatter.ISO_DATE_TIME)+"]"+string+"\n");
            writer.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
