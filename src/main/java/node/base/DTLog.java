package node.base;

import messages.Message;
import messages.TokenReader;
import messages.TokenWriter;

import java.io.IOException;
import java.io.Writer;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Ethan Petuchowski 2/26/15
 */
public abstract class DTLog {

    protected Writer writer;
    Node node;

    protected DTLog(Node node) {
        this.node = node;
    }

    LineTokenWriter currentLine = null;

    public TokenWriter beginLog() {
    	if (currentLine == null) {
    		currentLine = new LineTokenWriter();
    		return currentLine;
    	}
    	else
    		throw new RuntimeException("Must end last line before beginning new one");
    }

    public void endLog(TokenWriter writer) {
    	if (writer == currentLine && currentLine != null) {
    		log(currentLine.getLine());
    		currentLine = null;
    	}
    	else
    		throw new RuntimeException("Cannot end log that way");
    }

    private class LineTokenWriter extends TokenWriter {
    	private StringBuilder builder = new StringBuilder();

    	public String getLine() {
    		return builder.toString();
    	}

		@Override
		public void writeToken(String token) {
			builder.append(token);
			builder.append("  ");
		}
    }

    public Collection<Message> getLoggedMessages() {
    	ArrayList<Message> messages = new ArrayList<>();
    	String log = getLogAsString();
    	String[] logLines = log.split("\n");
    	for (String line : logLines) {
            if (line.isEmpty()) continue;
            String messageString = line.substring(line.lastIndexOf(']') + 2);
    		String[] messageTokens = messageString.split("  ");

    		TokenReader reader = new TokenReader() {
    			int index = 0;
				@Override
				public String readToken() {
					if (index < messageTokens.length) {
						return messageTokens[index++];
					}
					return null;
				}
    		};
    		messages.add(Message.readMessage(reader));
    	}
    	return messages;
    }

    private void log(String string) {
        try {
            /* SAMPLE LOG
            [3 2:32:34.3545] 2 ADD song_1 url_1
            [3 2:32:34.3545] 5 SENT Yes vote
            ...

            */
            String logOutput = String.format(
                    "[%d %s] %s\n",
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
