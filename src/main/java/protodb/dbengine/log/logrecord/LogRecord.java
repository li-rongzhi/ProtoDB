package protodb.dbengine.log.logrecord;

import javafx.util.Pair;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.page.BlockId;
import protodb.dbengine.xact.Transaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface LogRecord {
    static final int CHECKPOINT = 0, START = 1,
            COMMIT = 2, ROLLBACK  = 3,
            UPDATE = 4;

    /**
     * Returns the log record's type.
     * @return the log record's type
     */
    int op();

    /**
     * Returns the transaction id stored with
     * the log record.
     * @return the log record's transaction id
     */
    int txNumber();

    /**
     * Undoes the operation encoded by this log record.
     * The only log record types for which this method
     * does anything interesting are SETINT and SETSTRING.
     * @param tx the transaction that is performing the undo.
     */
    void undo(Transaction tx);

    int writeToLogFile(LogMgr lm);

    static LogRecord decodeLogRecord(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int type = bb.getInt(0);
        if (type == CHECKPOINT) {
            return new CheckpointRecord();
        }
        int txnum = bb.getInt(Integer.BYTES);
        switch (type) {
            case START:
                return new StartRecord(txnum);
            case COMMIT:
                return new CommitRecord(txnum);
            case ROLLBACK:
                return new RollbackRecord(txnum);
            case UPDATE:
                int fileNameLength = bb.getInt();
                byte[] fileNameBytes = new byte[fileNameLength];
                bb.get(fileNameBytes);
                String fileName = new String(fileNameBytes, StandardCharsets.UTF_8);
                int blkNum = bb.getInt();
                BlockId blk = new BlockId(fileName, blkNum);

                int offset = bb.getInt();

                int oldValLength = bb.getInt();
                byte[] oldVal = new byte[oldValLength];
                bb.get(oldVal);

                int newValLength = bb.getInt();
                byte[] newVal = new byte[newValLength];
                bb.get(newVal);

                return new UpdateRecord(txnum, blk, offset, oldVal, newVal);
            default:
                return null;
        }
    }

}
