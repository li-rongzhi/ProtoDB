package protodb.dbengine.log;

import protodb.dbengine.buffer.BufferMgr;
import protodb.dbengine.file.FileMgr;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.metadata.MetadataMgr;
import protodb.dbengine.xact.Transaction;
import protodb.dbengine.log.logrecord.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static protodb.dbengine.log.logrecord.LogRecord.*;

/**
 * The recovery manager.  Each transaction has its own recovery manager.
 */
public class RecoveryMgr {
    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;
    private Map<Integer, Transaction> activeXact;

    public RecoveryMgr(LogMgr lm, BufferMgr bm, FileMgr fm) {
        this.lm = lm;
        this.bm = bm;
        this.fm = fm;
        this.activeXact = new HashMap<>();
    }

    public void recover(MetadataMgr mdm) {
//        redo(mdm);
//        undo();
        new CheckpointRecord().writeToLogFile(lm);
    }
    public void redo(MetadataMgr mdm) {
        // fetch all log records since last checkpoint
        // redo everything
        // add to the activeXact list and remove finished xact
        Iterator<byte[]> iter = lm.getRecords(mdm, new Transaction(fm, lm, bm));
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.decodeLogRecord(bytes);
            if (rec.op() == START) {
                this.activeXact.put(rec.txNumber(), new Transaction(fm, lm, bm, rec.txNumber()));
                continue;
            }
            Transaction xact = this.activeXact.get(rec.txNumber());
            if (rec.op() == COMMIT) {
                xact.commit();
                this.activeXact.remove(rec.txNumber());
            } else if (rec.op() == ROLLBACK) {
                xact.rollback();
                this.activeXact.remove(rec.txNumber());
            } else if (rec.op() == UPDATE){
                UpdateRecord updateRec = (UpdateRecord) rec;
                xact.update(updateRec.getBlk(), updateRec.getOffset(),
                        updateRec.getNewVal(), false);
            }
        }
    }

    public void undo() {
        for (Map.Entry<Integer, Transaction> entry : activeXact.entrySet()) {
            entry.getValue().rollback();
        }
    }
}
