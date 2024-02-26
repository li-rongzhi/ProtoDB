package protodb.dbengine.file;

import protodb.dbengine.page.BlockId;
import protodb.dbengine.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 *  FileMgr is a class that manages file operations for a database,
 *  including reading, writing, and appending blocks to files.
 *  This class also handles the initialization of the database directory
 *  and the cleanup of temporary tables.
 */
public class FileMgr {
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();
        // create the directory if the database is new
        if (isNew) {
            dbDirectory.mkdirs();
        }
        // remove any leftover temporary tables
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    /**
     * Read the content of a block in the disk to a Page.
     * @param blk the BlockId of the target block to be read.
     * @param p the page that stores the content.
     */
    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * this.blockSize);
            f.getChannel().read(p.contents());
        }
        catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    /**
     * Write the content of a Page to a block in the disk.
     * @param blk the BlockId of the target block to be written.
     * @param p the page that stores the content.
     */
    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * this.blockSize);
            f.getChannel().write(p.contents());
        }
        catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }

    /**
     * Append a block at the end of a file.
     * @param filename the file to be appended.
     * @return the BlockId of the newly appended block.
     */
    public synchronized BlockId append(String filename) {
        int newBlkNum = this.length(filename);
        BlockId blk = new BlockId(filename, newBlkNum);
        byte[] b = new byte[this.blockSize];

        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * this.blockSize);
            f.write(b);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    /**
     * Calculate current number of blocks in the file.
     */
    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / this.blockSize);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
