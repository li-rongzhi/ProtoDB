package protodb.dbengine.log.logrecord;

import javafx.util.Pair;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.xact.Transaction;

import java.nio.ByteBuffer;

public class RollbackRecord implements LogRecord {
    private int txnum;
    public RollbackRecord(int txnum) {
        this.txnum = txnum;
    }
    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int txNumber() {
        return this.txnum;
    }

    @Override
    public void undo(Transaction tx) {}

    @Override
    public int writeToLogFile(LogMgr lm) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.BYTES);
        buffer.putInt(ROLLBACK);
        buffer.putInt(txnum);
        byte[] rec = buffer.array();
        return lm.append(rec);
    }

    @Override
    public String toString() {
        return "<ROLLBACK " + this.txnum + ">";
    }
}
