package system.node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Ethan Petuchowski 2/26/15
 */
public class DTLog {
    File file;
    FileWriter writer;
    DTLog(File file) {
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
            writer.write(string+"\n");
            writer.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
