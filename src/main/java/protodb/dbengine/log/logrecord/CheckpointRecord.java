package protodb.dbengine.log.logrecord;

import javafx.util.Pair;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.xact.Transaction;

import java.nio.ByteBuffer;

public class CheckpointRecord implements LogRecord {
    public CheckpointRecord() {
    }

    public int op() {
        return CHECKPOINT;
    }

    /**
     * Checkpoint records have no associated transaction,
     * and so the method returns a "dummy", negative txid.
     */
    public int txNumber() {
        return -1; // dummy value
    }

    /**
     * Does nothing, because a checkpoint record
     * contains no undo information.
     */
    public void undo(Transaction tx) {}

    @Override
    public int writeToLogFile(LogMgr lm) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(CHECKPOINT);
        byte[] rec = buffer.array();
        return lm.append(rec);
    }

    public String toString() {
        return "<CHECKPOINT>";
    }
}
