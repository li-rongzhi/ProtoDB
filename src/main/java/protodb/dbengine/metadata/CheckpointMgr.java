package protodb.dbengine.metadata;

import protodb.dbengine.record.Layout;
import protodb.dbengine.record.Schema;
import protodb.dbengine.record.TableScan;
import protodb.dbengine.xact.Transaction;

import java.util.Map;

public class CheckpointMgr {
    private TableMgr tblMgr;


    public CheckpointMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        if (isNew) {
            Schema sch = new Schema();
            sch.addIntField("lsn");
//            sch.addIntField("blkNum");
//            sch.addIntField("slot");
            tblMgr.createTable("checkpointct", sch, tx);
        }
    }

    public void addCheckpoint(int lsn, Transaction tx) {
        Layout layout = tblMgr.getLayout("checkpointct", tx);
        TableScan ts = new TableScan(tx, "checkpointct", layout);
        ts.insert();
        ts.setInt("lsn", lsn);
        ts.close();
    }

    public int getLatestCheckpoint(Transaction tx) {
        int result = -1;
        Layout layout = tblMgr.getLayout("checkpointct", tx);
        TableScan ts = new TableScan(tx, "checkpointct", layout);
        while (ts.next()) {
            result = ts.getInt("lsn");
        };
        ts.close();
        return result;
    }
}
