package protodb.dbengine.metadata;


import protodb.dbengine.record.Layout;
import protodb.dbengine.record.Schema;
import protodb.dbengine.xact.Transaction;

import java.util.Map;

public class MetadataMgr {
   private static TableMgr  tblmgr;
   private static ViewMgr   viewmgr;
   private static StatMgr   statmgr;
   private static IndexMgr  idxmgr;
   private static CheckpointMgr cpmgr;
   
   public MetadataMgr(boolean isnew, Transaction tx) {
      tblmgr  = new TableMgr(isnew, tx);
      viewmgr = new ViewMgr(isnew, tblmgr, tx);
      statmgr = new StatMgr(tblmgr, tx);
      idxmgr  = new IndexMgr(isnew, tblmgr, statmgr, tx);
      cpmgr = new CheckpointMgr(isnew, tblmgr, tx);
   }
   
   public void createTable(String tblname, Schema sch, Transaction tx) {
      tblmgr.createTable(tblname, sch, tx);
   }
   
   public Layout getLayout(String tblname, Transaction tx) {
      return tblmgr.getLayout(tblname, tx);
   }
   
   public void createView(String viewname, String viewdef, Transaction tx) {
      viewmgr.createView(viewname, viewdef, tx);
   }
   
   public String getViewDef(String viewname, Transaction tx) {
      return viewmgr.getViewDef(viewname, tx);
   }
   
   public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
      idxmgr.createIndex(idxname, tblname, fldname, tx);
   }
   
   public Map<String,IndexInfo> getIndexInfo(String tblname, Transaction tx) {
      return idxmgr.getIndexInfo(tblname, tx);
   }
   
   public StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
      return statmgr.getStatInfo(tblname, layout, tx);
   }

   public void addCheckpoint(int lsn, Transaction tx) {
      cpmgr.addCheckpoint(lsn, tx);
   }

   public int getLatestCheckpoint(Transaction tx) {
      return cpmgr.getLatestCheckpoint(tx);
   }
}
