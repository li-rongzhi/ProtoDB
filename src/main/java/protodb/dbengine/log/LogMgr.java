package protodb.dbengine.log;


import protodb.dbengine.file.*;
import protodb.dbengine.metadata.MetadataMgr;
import protodb.dbengine.page.BlockId;
import protodb.dbengine.page.LogPage;
import protodb.dbengine.xact.Transaction;

import java.util.Iterator;

public class LogMgr {
    private FileMgr fm;
    private String logfile;
    private LogPage logpage;
    private BlockId currentblk;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    /**
     * Creates the manager for the specified log file.
     * If the log file does not yet exist, it is created
     * with an empty first block.
     * @param fm the file manager
     * @param logfile the name of the log file
     */
    public LogMgr(FileMgr fm, String logfile) {
        this.fm = fm;
        this.logfile = logfile;
        byte[] b = new byte[fm.blockSize()];
        this.logpage = new LogPage(b);
        int logsize = fm.length(logfile);
        if (logsize == 0)
            currentblk = appendNewBlock();
        else {
            currentblk = new BlockId(logfile, logsize-1);
            fm.read(currentblk, this.logpage);
        }
        System.out.println("LogMgr successfully created");
    }

    /**
     * Ensures that the log record corresponding to the
     * specified LSN has been written to disk.
     * All earlier log records will also be written to disk.
     * @param lsn the LSN of a log record
     */
    public void flush(int lsn) {
        if (lsn >= lastSavedLSN)
            flush();
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentblk);
    }

    /**
     * Appends a log record to the log buffer.
     * The record consists of an arbitrary array of bytes.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location
     * of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read
     * them in reverse order.
     * @param logrec a byte buffer containing the bytes.
     * @return the LSN of the final value
     */
    public synchronized int append(byte[] logrec) {
        int boundary = logpage.getInt(0);
        int recsize = logrec.length;
        int bytesneeded = recsize + Integer.BYTES;
        if (boundary - bytesneeded < Integer.BYTES) { // the log record doesn't fit,
            flush();        // so move to the next block.
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recpos = boundary - bytesneeded;

        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos); // the new boundary
        latestLSN += 1;
        return latestLSN;
    }

    /**
     * Initialize the bytebuffer and append it to the log file.
     */
    private BlockId appendNewBlock() {
        BlockId blk = fm.append(logfile);
        logpage.setInt(0, fm.blockSize());
        fm.write(blk, logpage);
        return blk;
    }

    /**
     * Write the buffer to the log file.
     */
    private void flush() {
        fm.write(currentblk, logpage);
        lastSavedLSN = latestLSN;
    }

    public LogIterator getRecords(MetadataMgr mm, Transaction tx) {
        // Todo!
        int lsn = mm.getLatestCheckpoint(tx);
        if (lsn == -1)
            return new LogIterator(fm, currentblk);
        else {
            LogIterator tmp = new LogIterator(fm, currentblk);
            return tmp;
        }


    }
}
