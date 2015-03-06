package node.system;

import node.base.DTLog;
import node.base.Node;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Ethan Petuchowski 2/27/15
 */
public class FileDTLog extends DTLog {
    protected FileDTLog(File file, Node node) {
        super(node);
        this.file = file;
        try {
            if (!file.getParentFile().exists())
                file.getParentFile().mkdirs();

            if (!file.exists())
                file.createNewFile();
            writer = new FileWriter(file);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    File file;

    @Override public String getLogAsString() {
        try {
            return new String(Files.readAllBytes(file.toPath()));
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
