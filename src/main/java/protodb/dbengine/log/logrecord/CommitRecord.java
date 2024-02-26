package protodb.dbengine.log.logrecord;

import javafx.util.Pair;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.xact.Transaction;

import java.nio.ByteBuffer;

public class CommitRecord implements LogRecord {
    private int txnum;
    public CommitRecord(int txnum) {
        this.txnum = txnum;
    }
    @Override
    public int op() {
        return COMMIT;
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
        buffer.putInt(COMMIT);
        buffer.putInt(txnum);
        byte[] rec = buffer.array();
        return lm.append(rec);
    }

    @Override
    public String toString() {
        return "<COMMIT " + this.txnum + ">";
    }
}
