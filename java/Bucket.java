import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Bucket {
    private long id;
    private String name;
    private String path;
    private AtomicLong capacity = new AtomicLong();
    private long maximumCapacity;
    private AtomicLong position = new AtomicLong();
    private long threshold;
    private boolean writable;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    public Bucket(long id, String name, String path, long maximumCapacity) {
        this.id = id;
        this.name = name;
        this.path = path;
        this.maximumCapacity = maximumCapacity;

        File f = new File(path);
        if (!f.exists()) {
            try {
                new RandomAccessFile(f, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void init() {
    }

    public void write(byte[] bs, long position) throws IOException {
        if (!writable) throw new IOException("can not write");
        long len = bs.length;
        if (position > maximumCapacity || position + len > maximumCapacity) throw new IOException("can not write");
        Lock lock = rwl.writeLock();
        lock.lock();
        try {
            FileUtils.execute(new File(path), "rw", position, len, f -> {
                f.seek(position);
                f.write(bs);
                this.capacity.getAndAdd(len);
            });
            this.position.getAndAdd(len);
        } finally {
            lock.unlock();
        }
    }

    public byte[] read(int position, int len) throws IOException {
        if (position > capacity.get() || position + len > capacity.get())
            throw new IOException(String.format("out of range: %d - {%d : %d}", this.capacity.get(), position, len));
        Lock lock = rwl.readLock();
        lock.lock();
        try {
            return FileUtils.read(new File(path), "r", f -> {
                byte[] bytes = new byte[len];
                f.seek(position);
                if (f.read(bytes, 0, len) != -1) {
                    return bytes;
                }
                throw new IOException(String.format("out of range: %d - {%d : %d }", this.capacity.get(), position, len));
            });
        } finally {
            lock.unlock();
        }
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getCapacity() {
        return capacity.get();
    }

    public long getMaximumCapacity() {
        return maximumCapacity;
    }

    public void setMaximumCapacity(long maximumCapacity) {
        this.maximumCapacity = maximumCapacity;
    }

    public long getPosition() {
        return position.get();
    }

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public boolean isWritable() {
        return writable;
    }

    public void setWritable(boolean writable) {
        this.writable = writable;
    }
}

final class FileUtils {
    static boolean isWindows = System.getProperty("os.name").contains("Windows");

    public static interface Runnable {
        void run(RandomAccessFile file) throws IOException;
    }

    public static interface Callable<T> {
        T call(RandomAccessFile file) throws IOException;
    }

    static FileLock getLock(FileChannel channel, long position, long len) {
        FileLock lock = null;
        while (lock == null) {
            try {
                if (isWindows) {
                    lock = channel.lock();
                } else {
                    lock = channel.lock(position, len, true);
                }
            } catch (Exception e) {
            }
        }
        return lock;
    }

    static void execute(File file, String mode, long position, long len, @NotNull Runnable callback) throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(file, mode)) {
            FileLock lock = getLock(f.getChannel(), position, len);
            try {
                callback.run(f);
            } finally {
                lock.release();
            }
        }
    }

    static <T> T read(File file, String mode, @NotNull Callable<T> callback) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, mode)) {
            return callback.call(raf);
        }
    }

}