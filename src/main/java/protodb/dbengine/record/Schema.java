package protodb.dbengine.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.sql.Types.*;

public class Schema {
    class FieldInfo {
        int type, length;
        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
    private List<String> fields;
    private Map<String, FieldInfo>  info;
    public Schema() {
        this.fields = new ArrayList<>();
        this.info = new HashMap<>();
    };
    public void addField(String fldname, int type, int length) {
        // Todo: Handling duplicate
        this.fields.add(fldname);
        this.info.put(fldname, new FieldInfo(type, length));
    };

    public void addIntField(String fldname) {
        this.addField(fldname, INTEGER, 0);
    };
    public void addStringField(String fldname, int length) {
        this.addField(fldname, VARCHAR, length);
    };

    public void add(String fldname, Schema sch) {
        int type   = sch.type(fldname);
        int length = sch.length(fldname);
        addField(fldname, type, length);
    };
    public void addAll(Schema sch) {
        for (String fldname : sch.fields())
            add(fldname, sch);
    };

    public List<String> fields() {
        return this.fields;
    };

    public boolean hasField(String fldname) {
        return this.fields.contains(fldname);
    };

    public int type(String fldname) {
        return this.info.get(fldname).type;
    };
    public int length(String fldname) {
        return this.info.get(fldname).length;
    };
}
