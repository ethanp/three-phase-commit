package node.mock;

import java.util.Queue;

import messages.TokenWriter;

public class QueueTokenWriter extends TokenWriter {
	private Queue<String> queue;
	
	public QueueTokenWriter(Queue<String> queue)
	{
		this.queue = queue;
	}
	
	@Override
	public void writeToken(String token) {
		// TODO Auto-generated method stub
		queue.add(token);
	}
}
