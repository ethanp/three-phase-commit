package messages;

public class PeerTimeout extends Message {
	private int peerId;
	
	public PeerTimeout(int id) {
		super(Command.TIMEOUT, -1);
		peerId = id;
	}
	
	public int getPeerId() {
		return peerId;
	}
	
	@Override
	protected void writeAsTokens(TokenWriter writer) {
		writer.writeToken(new Integer(peerId).toString());
	}

	@Override
	protected void readFromTokens(TokenReader reader) {
		peerId = Integer.parseInt(reader.readToken());
	}
}
