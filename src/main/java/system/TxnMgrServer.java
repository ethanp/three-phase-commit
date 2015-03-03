package system;

import system.network.ObjectConnection;
import util.Common;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Ethan Petuchowski 2/26/15
 */
public class TxnMgrServer implements Runnable {
    AsyncTxnMgr txnMgr;
    ServerSocket serverSocket;
    static int requestPort = 3000;

    public int getListenPort() {
        return serverSocket.getLocalPort();
    }

    public TxnMgrServer(AsyncTxnMgr txnMgr) {
        this.txnMgr = txnMgr;
        serverSocket = Common.claimOpenPort();
    }

    @Override public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                ObjectConnection conn = ObjectConnection.txnMgrAddConnection(socket, txnMgr);

                /* transactions can commence once all nodes have connected */
                txnMgr.nodesConnected.lockInterruptibly();
                if (txnMgr.getNumConnectedNodes() == txnMgr.getNodes().size()) {
                    txnMgr.allNodesConnected.signalAll();
                }
                txnMgr.nodesConnected.unlock();
            }
            catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
