package messages;

public class PeerTimeout extends Message {
	private int peerId;

	public PeerTimeout(int peerID) {
		super(Command.TIMEOUT, -1);
		this.peerId = peerID;
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
