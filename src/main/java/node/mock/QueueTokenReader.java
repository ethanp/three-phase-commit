package node.mock;

import java.util.Queue;

import messages.TokenReader;

public class QueueTokenReader extends TokenReader {
	private Queue<String> queue;
	
	public QueueTokenReader(Queue<String> queue)
	{
		this.queue = queue;
	}

	@Override
	public String readToken() {
		return queue.poll();
	}
}
