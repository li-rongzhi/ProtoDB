package protodb.dbengine.log;

import protodb.dbengine.file.*;
import protodb.dbengine.page.BlockId;
import protodb.dbengine.page.LogPage;

import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private FileMgr fm;
    private BlockId blk;
    private LogPage p;
    private int currentpos;
    private int boundary;

    /**
     * Creates an iterator for the records in the log file,
     * positioned after the last log record.
     */
    public LogIterator(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        byte[] b = new byte[fm.blockSize()];
        p = new LogPage(b);
        moveToBlock(blk);
    }

    /**
     * Determines if the current log record
     * is the earliest record in the log file.
     * @return true if there is an earlier record
     */
    public boolean hasNext() {
        return currentpos<fm.blockSize() || blk.number()>0;
    }

    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    public byte[] next() {
        if (currentpos == fm.blockSize()) {
            blk = new BlockId(blk.fileName(), blk.number()-1);
            moveToBlock(blk);
        }
        byte[] rec = p.getBytes(currentpos);
        currentpos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(BlockId blk) {
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentpos = boundary;
    }
}
