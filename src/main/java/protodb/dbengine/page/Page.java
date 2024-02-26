package protodb.dbengine.page;

import java.nio.ByteBuffer;

public interface Page {
    int getInt(int offset);
    void setInt(int offset, int n);
    byte[] getBytes(int offset);
    void setBytes(int offset, byte[] b);
    String getString(int offset);
    void setString(int offset, String s);
    ByteBuffer contents();
}

