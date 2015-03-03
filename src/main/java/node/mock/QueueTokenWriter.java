package node.mock;

import messages.TokenWriter;

import java.util.Queue;

public class QueueTokenWriter extends TokenWriter {
	private Queue<String> queue;

	public QueueTokenWriter(Queue<String> queue)
	{
		this.queue = queue;
	}

	@Override
	public void writeToken(String token) {
		queue.add(token);
	}
}
