package protodb.dbengine.xact;

import protodb.dbengine.buffer.Buffer;
import protodb.dbengine.buffer.BufferMgr;
import protodb.dbengine.concurrency.ConcurrencyMgr;
import protodb.dbengine.file.FileMgr;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.log.logrecord.CommitRecord;
import protodb.dbengine.log.logrecord.LogRecord;
import protodb.dbengine.log.logrecord.RollbackRecord;
import protodb.dbengine.log.logrecord.UpdateRecord;
import protodb.dbengine.page.BlockId;
import protodb.dbengine.page.DataPage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static protodb.dbengine.log.logrecord.LogRecord.UPDATE;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;
    private int txnum;
    private BufferList mybuffers;
    private ConcurrencyMgr cm;

    private ArrayList<byte[]> ops;

    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm){
        this.fm = fm;
        this.lm = lm;
        this.bm = bm;
        this.txnum = nextTxNumber();
        this.cm   = new ConcurrencyMgr();
        this.mybuffers = new BufferList(bm);
    };

    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm, int txnum){
        this.fm = fm;
        this.lm = lm;
        this.bm = bm;
        this.txnum = txnum;
        this.cm   = new ConcurrencyMgr();
        this.mybuffers = new BufferList(bm);
    };

    public void commit() {
        CommitRecord record = new CommitRecord(txnum);
        int lsn = record.writeToLogFile(lm);
        lm.flush(lsn);
        bm.flushAll(txnum);
        System.out.println("transaction " + txnum + " committed");
        this.cm.release();
        this.mybuffers.unpinAll();
    };

    public void rollback() {
        RollbackRecord record = new RollbackRecord(txnum);
        int lsn = record.writeToLogFile(lm);
        lm.flush(lsn);
        doRollback();
        bm.flushAll(txnum);
        System.out.println("transaction " + this.txnum + " rolled back");
        this.cm.release();
        this.mybuffers.unpinAll();
    };

    public void doRollback() {
        for (int i = this.ops.size() - 1; i >= 0; i--) {
            byte[] operation = this.ops.get(i);
            LogRecord rec = LogRecord.decodeLogRecord(operation);
            rec.undo(this);
        }
    }

    public void pin(BlockId blk) {
        this.mybuffers.pin(blk);
    };
    public void unpin(BlockId blk) {
        this.mybuffers.unpin(blk);
    };

    public int getInt(BlockId blk, int offset) {
        this.cm.sLock(blk);
        Buffer buff = this.mybuffers.getBuffer(blk);
        return buff.contents().getInt(offset);
    };
    public String getString(BlockId blk, int offset) {
        this.cm.sLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        return buff.contents().getString(offset);
    };
    public void setInt(BlockId blk, int offset, int val,
                       boolean needLog) {
        cm.xLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        int lsn = -1;
        if (needLog) {
            byte[] oldVal = buff.contents().getBytes(offset);
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
            byteBuffer.putInt(val); // Put the integer into the ByteBuffer
            byte[] bytes = byteBuffer.array();
            UpdateRecord record = new UpdateRecord(txnum, blk, offset, oldVal, bytes);
            lsn = record.writeToLogFile(lm);
        }
        DataPage p = buff.contents();
        p.setInt(offset, val);
        buff.setModified(txnum, lsn);
    };
    public void setString(BlockId blk, int offset, String val,
                          boolean needLog) {
        cm.xLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        int lsn = -1;
        if (needLog) {
            byte[] oldVal = buff.contents().getBytes(offset);
            UpdateRecord record = new UpdateRecord(txnum, blk, offset, oldVal, val.getBytes());
            lsn = record.writeToLogFile(lm);
        }
        DataPage p = buff.contents();
        p.setString(offset, val);
        buff.setModified(txnum, lsn);
    };

    public void update(BlockId blk, int offset, byte[] val, boolean needLog) {
        this.cm.xLock(blk);
        Buffer buff = mybuffers.getBuffer(blk);
        int lsn = -1;
        if (needLog) {
            byte[] oldVal = buff.contents().getBytes(offset);
            UpdateRecord record = new UpdateRecord(txnum, blk, offset, oldVal, val);
            lsn = record.writeToLogFile(lm);
        }
        DataPage p = buff.contents();
        p.setBytes(offset, val);
        buff.setModified(txnum, lsn);
        this.ops.add(this.generateLog(buff, offset, val));
    }


    public int availableBuffs() {
        return bm.available();
    };
    public int size(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        this.cm.sLock(dummyblk);
        return fm.length(filename);
    };

    public BlockId append(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        this.cm.xLock(dummyblk);
        return fm.append(filename);
    };

    public int blockSize() {
        return fm.blockSize();
    };

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        return nextTxNum;
    }

    public byte[] generateLog(Buffer buff, int offset, byte[] newVal) {
        BlockId blk = buff.block();
        byte[] oldVal = buff.contents().getBytes(offset);
        ByteBuffer buffer = ByteBuffer.allocate(
                7 * Integer.BYTES + blk.fileName().length()
                        + oldVal.length + newVal.length);
        buffer.putInt(UPDATE);
        buffer.putInt(this.txnum);
        buffer.putInt(blk.fileName().length());
        buffer.put(blk.fileName().getBytes(StandardCharsets.UTF_8));
        buffer.putInt(blk.number());
        buffer.putInt(offset);
        buffer.putInt(oldVal.length);
        buffer.put(oldVal);
        buffer.putInt(newVal.length);
        buffer.put(newVal);
        return buffer.array();
    }
}
