package protodb.dbengine.plan;

import protodb.dbengine.parser.QueryData;
import protodb.dbengine.xact.Transaction;

public interface QueryPlanner {
    public Plan createPlan(QueryData data, Transaction tx);
}
