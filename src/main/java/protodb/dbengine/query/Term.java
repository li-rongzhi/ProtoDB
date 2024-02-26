package protodb.dbengine.query;


import protodb.dbengine.plan.Plan;
import protodb.dbengine.record.Schema;

public class Term {
   private Expression lhs, rhs;

   public Term(Expression lhs, Expression rhs) {
      this.lhs = lhs;
      this.rhs = rhs;
   }

   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      return rhsval.equals(lhsval);
   }

   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }

   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }

   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }

   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   public String toString() {
      return lhs.toString() + "=" + rhs.toString();
   }
}
