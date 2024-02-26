package protodb.dbengine;

import protodb.dbengine.buffer.BufferMgr;
import protodb.dbengine.file.FileMgr;
import protodb.dbengine.log.LogMgr;
import protodb.dbengine.metadata.MetadataMgr;
import protodb.dbengine.plan.*;
import protodb.dbengine.log.RecoveryMgr;
import protodb.dbengine.xact.Transaction;

import java.io.File;

public class ProtoDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private FileMgr fm;
    private BufferMgr bm;
    private LogMgr lm;
    private MetadataMgr mdm;
    private Planner planner;
    private RecoveryMgr rm;

    public ProtoDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
    }

    public ProtoDB(String dirname) {
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();
        boolean isnew = fm.isNew();
        mdm = new MetadataMgr(isnew, tx);
        rm = new RecoveryMgr(lm, bm, fm);
        if (isnew)
            System.out.println("creating new database");
        else {
            System.out.println("recovering existing database");
//            tx.recover();
            rm.recover(mdm);
        }

        QueryPlanner qp = new BasicQueryPlanner(mdm);
        UpdatePlanner up = new BasicUpdatePlanner(mdm);
        planner = new Planner(qp, up);
        tx.commit();
    }

    /**
     * A convenient way for clients to create transactions
     * and access the metadata.
     */
    public Transaction newTx() {
        return new Transaction(fm, lm, bm);
    }

    public MetadataMgr mdMgr() {
        return mdm;
    }

    public Planner planner() {
        return planner;
    }

    // These methods aid in debugging
    public FileMgr fileMgr() {
        return fm;
    }
    public LogMgr logMgr() {
        return lm;
    }
    public BufferMgr bufferMgr() {
        return bm;
    }
}
