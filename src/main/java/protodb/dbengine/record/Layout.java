package protodb.dbengine.record;

import protodb.dbengine.page.DataPage;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;

public class Layout {
    private Schema schema;
    private Map<String,Integer> offsets;
    private int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        offsets  = new HashMap<>();
        int pos = Integer.BYTES; // leave space for the empty/inuse flag
        for (String fldname : schema.fields()) {
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }
        slotSize = pos;
    }

    public Layout(Schema schema, Map<String,Integer> offsets, int slotSize) {
        this.schema    = schema;
        this.offsets   = offsets;
        this.slotSize = slotSize;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String fldname) {
        return offsets.get(fldname);
    }


    public int slotSize() {
        return slotSize;
    }

    public int lengthInBytes(String fldname) {
        int fldtype = schema.type(fldname);
        if (fldtype == INTEGER)
            return Integer.BYTES;
        else // fldtype == VARCHAR
            return DataPage.maxLength(schema.length(fldname));
    }
}
