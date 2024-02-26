package protodb.dbengine.buffer;

import protodb.dbengine.file.FileMgr;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.page.*;

public class Buffer {
    private FileMgr fm;
    private LogMgr lm;
    private DataPage contents;
    private BlockId blk = null;
    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    public Buffer(FileMgr fm, LogMgr lm) {
        this.fm = fm;
        this.lm = lm;
        this.contents = new DataPage(fm.blockSize());
    }

    public DataPage contents() {
        return contents;
    }

    public BlockId block() {
        return blk;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0)
            this.lsn = lsn;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    void assignToBlock(BlockId b) {
        flush();
        blk = b;
        fm.read(blk, contents);
        pins = 0;
    }

    void flush() {
        if (txnum >= 0) {
            lm.flush(lsn);
            fm.write(blk, contents);
            txnum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
