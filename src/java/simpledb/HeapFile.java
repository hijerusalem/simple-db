package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public int pageSize() {
        return BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(f));
            int pageSize = pageSize();
            byte pageBuf[] = new byte[pageSize];
            bis.skip(pid.getPageNumber()  * pageSize);
            bis.read(pageBuf, 0, pageSize);
            return new HeapPage((HeapPageId)pid, pageBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (bis != null)
                    bis.close();
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile ras = null;
        try {

            ras = new RandomAccessFile(f, "rw");
            int pageSize = pageSize();
            byte pageData[] = page.getPageData();
            int offset = page.getId().getPageNumber() * pageSize;
            ras.seek(offset);
            ras.write(pageData, 0, pageSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (ras != null)
                    ras.close();
            } catch (IOException ioe) {
                // Ignore failures closing the file
            }

        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / pageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int numPages = this.numPages();
        HeapPage page = null;
        for (int i=0; i<numPages; i++) {
            page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                break;
            }
        }
        boolean newPage = false;
        if (page.getNumEmptySlots() == 0) {
            byte[] data = HeapPage.createEmptyPageData();
            page = new HeapPage(new HeapPageId(this.getId(), numPages), data);
            newPage = true;
        }

        page.insertTuple(t);
        if (newPage) {
            this.writePage(page);
            return null;
        }
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new Itr();
    }

    private class Itr implements DbFileIterator {
        int cursor = -1;
        HeapPage currentPage = null;
        Iterator<Tuple> currentTupleIterator = null;
        TransactionId tid = null;
        int tableid;
        Boolean open = false;
        int numPages;

        public void open() throws DbException, TransactionAbortedException {
            open = true;
            cursor = 0;
            numPages = numPages();
            tableid = getId();
            tid = new TransactionId();
            HeapPageId hid = new HeapPageId(tableid, cursor);
            try {
                currentPage = (HeapPage) Database.getBufferPool().getPage(tid, hid, Permissions.READ_WRITE);
                currentTupleIterator = currentPage.iterator();
            } catch (Exception e) {
                throw e;
            }
        }

        public boolean hasNext() {
            if (!open)
                return false;

            while (cursor < numPages) {
                if (currentPage == null) {
                    HeapPageId hid = new HeapPageId(tableid, cursor);
                    try {
                        currentPage = (HeapPage) Database.getBufferPool().getPage(tid, hid, Permissions.READ_WRITE);
                    } catch (Exception e) {
                        return false;
                    }
                    currentTupleIterator = currentPage.iterator();
                }

                Boolean hasNext = currentTupleIterator.hasNext();
                if (hasNext)
                    return true;

                currentPage = null;
                cursor++;
            }

            return false;
        }

        public Tuple next() {
            if (!open)
                throw new NoSuchElementException();
            if (cursor < numPages)
                return currentTupleIterator.next();
            throw new NoSuchElementException();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            if (!open)
                throw new IllegalStateException("Heap file iterator not open yet");
            cursor = 0;
            HeapPageId hid = new HeapPageId(tableid, cursor);
            try {
                currentPage = (HeapPage) Database.getBufferPool().getPage(tid, hid, Permissions.READ_WRITE);
            } catch (Exception e) {
                throw e;
            }
            currentTupleIterator = currentPage.iterator();
        }

        public void close() {
            open = false;
        }

    }

}

