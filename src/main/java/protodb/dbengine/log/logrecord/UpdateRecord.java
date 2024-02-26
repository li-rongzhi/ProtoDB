package protodb.dbengine.log.logrecord;

import protodb.dbengine.log.LogMgr;
import protodb.dbengine.page.BlockId;
import protodb.dbengine.xact.Transaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class UpdateRecord implements LogRecord {
    private int txnum;
    private BlockId blk;
    private int offset;
    private byte[] oldVal;
    private byte[] newVal;

    public UpdateRecord(int txnum, BlockId blk, int offset,
                        byte[] oldVal, byte[] newVal) {
        this.txnum = txnum;
        this.blk = blk;
        this.offset = offset;
        this.oldVal = oldVal;
        this.newVal = newVal;
    }

    public BlockId getBlk() {
        return this.blk;
    }
    public int getOffset() {
        return this.offset;
    }

    public byte[] getNewVal() {
        return this.newVal;
    }
    @Override
    public int op() {
        return UPDATE;
    }

    @Override
    public int txNumber() {
        return this.txnum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(this.blk);
        tx.update(this.blk, this.offset, this.oldVal, false); // don't log the undo!
        tx.unpin(this.blk);
    }

    @Override
    public int writeToLogFile(LogMgr lm) {
        ByteBuffer buffer = ByteBuffer.allocate(
                7 * Integer.BYTES + this.blk.fileName().length()
                        + this.oldVal.length + this.newVal.length);
        buffer.putInt(UPDATE);
        buffer.putInt(this.txnum);
        buffer.putInt(this.blk.fileName().length());
        buffer.put(this.blk.fileName().getBytes(StandardCharsets.UTF_8));
        buffer.putInt(this.blk.number());
        buffer.putInt(this.offset);
        buffer.putInt(this.oldVal.length);
        buffer.put(this.oldVal);
        buffer.putInt(this.newVal.length);
        buffer.put(this.newVal);
        byte[] rec = buffer.array();
        return lm.append(rec);
    }
}
