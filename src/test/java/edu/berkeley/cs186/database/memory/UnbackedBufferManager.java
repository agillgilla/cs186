package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Buffer manager with no backing persistent store. Partition number is ignored.
 */
public class UnbackedBufferManager implements BufferManager {
    private Map<Long, byte[]> pages = new HashMap<>();
    private long nextPageNum = 0L;
    private boolean isClosed = false;
    private List<Frame> frames = new ArrayList<>();

    class Frame extends BufferFrame {
        long pageNum;

        Frame(long pageNum) {
            this.pageNum = pageNum;
        }

        @Override
        boolean isValid() {
            return !isClosed && pages.get(pageNum) != null;
        }

        @Override
        long getPageNum() {
            return pageNum;
        }

        @Override
        void flush() {}

        @Override
        void readBytes(short position, short num, byte[] buf) {
            if (!this.isValid()) {
                throw new IllegalStateException("reading from invalid buffer frame");
            }
            System.arraycopy(pages.get(pageNum), position, buf, 0, num);
        }

        @Override
        void writeBytes(short position, short num, byte[] buf) {
            if (!this.isValid()) {
                throw new IllegalStateException("reading from invalid buffer frame");
            }
            System.arraycopy(buf, 0, pages.get(pageNum), position, num);
        }

        @Override
        BufferFrame requestValidFrame() {
            this.pin();
            return this;
        }

        @Override
        long getPageLSN() {
            return 0;
        }

        @Override
        public String toString() {
            String s = "Buffer Frame for Page " + pageNum;
            if (isPinned()) {
                s += " (pinned)";
            }
            return s;
        }
    }

    @Override
    public void close() {
        isClosed = true;
        for (Frame frame : frames) {
            if (frame.isPinned()) {
                throw new IllegalStateException("frame still pinned on buffer manager close");
            }
        }
    }

    @Override
    public BufferFrame fetchPageFrame(long pageNum, boolean logPage) {
        if (pageNum < 0 || pageNum >= nextPageNum || pages.get(pageNum) == null) {
            throw new PageException("page " + pageNum + " not valid");
        }
        Frame frame = new Frame(pageNum);
        frame.pin();
        frames.add(frame);
        return frame;
    }

    @Override
    public Page fetchPage(LockContext parentContext, long pageNum, boolean logPage) {
        return new Page(new DummyLockContext(), fetchPageFrame(pageNum, logPage));
    }

    @Override
    public BufferFrame fetchNewPageFrame(int partNum, boolean logPage) {
        pages.put(nextPageNum, new byte[logPage ? DiskSpaceManager.PAGE_SIZE :
                                        BufferManager.EFFECTIVE_PAGE_SIZE]);
        Frame frame = new Frame(nextPageNum++);
        frame.pin();
        frames.add(frame);
        return frame;
    }

    @Override
    public Page fetchNewPage(LockContext parentContext, int partNum, boolean logPage) {
        return new Page(new DummyLockContext(), fetchNewPageFrame(partNum, logPage));
    }

    @Override
    public void freePage(Page page) {
        pages.put(page.getPageNum(), null);
    }

    @Override
    public void freePart(int partNum) {
        throw new UnsupportedOperationException("partition operations not supported");
    }

    @Override
    public void flushAll() {}

    @Override
    public void iterPageNums(Consumer<Long> process) {}
}
