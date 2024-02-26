package protodb.dbengine.plan;


import protodb.dbengine.metadata.MetadataMgr;
import protodb.dbengine.metadata.StatInfo;
import protodb.dbengine.query.Scan;
import protodb.dbengine.record.Layout;
import protodb.dbengine.record.Schema;
import protodb.dbengine.record.TableScan;
import protodb.dbengine.xact.Transaction;

/** The Plan class corresponding to a table.
  * @author Edward Sciore
  */
public class TablePlan implements Plan {
   private String tblname;
   private Transaction tx;
   private Layout layout;
   private StatInfo si;
   
   /**
    * Creates a leaf node in the query tree corresponding
    * to the specified table.
    * @param tblname the name of the table
    * @param tx the calling transaction
    */
   public TablePlan(Transaction tx, String tblname, MetadataMgr md) {
      this.tblname = tblname;
      this.tx = tx;
      layout = md.getLayout(tblname, tx);
      si = md.getStatInfo(tblname, layout, tx);
   }
   
   /**
    * Creates a table scan for this query.
    */
   public Scan open() {
      return new TableScan(tx, tblname, layout);
   }
   
   /**
    * Estimates the number of block accesses for the table,
    * which is obtainable from the statistics manager.
    */ 
   public int blocksAccessed() {
      return si.blocksAccessed();
   }
   
   /**
    * Estimates the number of records in the table,
    * which is obtainable from the statistics manager.
    */
   public int recordsOutput() {
      return si.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct field values in the table,
    * which is obtainable from the statistics manager.
    */
   public int distinctValues(String fldname) {
      return si.distinctValues(fldname);
   }
   
   /**
    * Determines the schema of the table,
    * which is obtainable from the catalog manager.
    */
   public Schema schema() {
      return layout.schema();
   }
}
