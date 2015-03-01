package node.mock;


import node.base.DTLog;
import node.base.Node;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;

/**
 * Ethan Petuchowski 2/27/15
 */
public class ByteArrayDTLog extends DTLog {
    public ByteArrayDTLog(Node node) {
        super(node);
        writer = new OutputStreamWriter(byteArray);
    }

    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();

    public String getLogAsString() {
        return byteArray.toString();
    }
}
