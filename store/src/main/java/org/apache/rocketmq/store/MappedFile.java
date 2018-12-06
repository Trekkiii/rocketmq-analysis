/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MappedFile extends ReferenceResource {

    // 默认页大小为4k
    public static final int OS_PAGE_SIZE = 1024 * 4;

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // JVM中映射的虚拟内存总大小
    // 简单说就是所有MappedFile实例已使用的字节总数
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // MappedFile的个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    // 当前MappedFile对象的当前写指针，当值等于fileSize代表文件写满了
    // 注意，这里记录的不是真正刷入磁盘的位置，而是写入到buffer的位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 当前提交的位置
    // 所谓提交就是将writeBuffer的脏数据写到fileChannel
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 当前刷盘的位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // mappedFile文件大小，参照MessageStoreConfig.mapedFileSizeCommitLog，默认1G
    protected int fileSize;
    // 对file进行包装，以支持其随机读写。
    // 通过fileChannel将此通道的文件区域直接映射到内存中，对应的内存映射为mappedByteBuffer，可以直接通过mappedByteBuffer操作commitLog。
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 从transientStorePool中获取，消息先写入该buffer，然后再写入到fileChannel。可能为null
    // 只有仅当transientStorePoolEnable为true，FlushDiskType为异步刷盘（ASYNC_FLUSH），并且为*_Master时，才启用。
    protected ByteBuffer writeBuffer = null;
    // ByteBuffer的缓冲池，一个CommitLog file对应一个DirectByteBuffer
    protected TransientStorePool transientStorePool = null;
    // 文件全路径名
    private String fileName;
    // commitLog文件起始偏移量
    // 其实就是文件名称，一般为20位数字，代表这个文件开始时的offset
    private long fileFromOffset;
    // 文件对象
    private File file;
    // fileChannel内存映射
    private MappedByteBuffer mappedByteBuffer;
    // 最后一次存储消息的时间戳
    private volatile long storeTimestamp = 0;
    // 是不是刚刚创建的
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    /**
     * 判断目录是否存在，如果不存在则创建目录
     *
     * @param dirName
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        // 通过反射获取所有方法
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 执行初始化
     * <p>
     * 设置transientStorePool；
     * 设置writeBuffer，从transientStorePool获取
     *
     * @param fileName
     * @param fileSize
     * @param transientStorePool
     * @throws IOException
     */
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 执行初始化
     * <p>
     * 设置文件全路径名；
     * 设置文件大小；
     * 设置fileFromOffset代表文件对应的偏移量；
     * 获取 fileChannel，mappedByteBuffer 相关IO对象；
     * TOTAL_MAPPED_VIRTUAL_MEMORY,TOTAL_MAPPED_FILES计数更新；
     *
     * @param fileName 文件全路径名
     * @param fileSize 文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName; // 设置文件全路径名
        this.fileSize = fileSize; // 设置文件大小
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName()); // 其实就是文件名称，一般为20位数字
        boolean ok = false;

        // 判断父目录是否存在，如果不存在则创建父目录
        ensureDirOK(this.file.getParent());

        try {
            // 对file进行包装，以支持其随机读写
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // fileChannel内存映射，将此通道的文件区域直接映射到内存中。
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 追加MessageExtBrokerInner消息
     *
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    /**
     * 追加MessageExtBatch消息
     *
     * @param messageExtBatch
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        // 参数非空校验
        assert messageExt != null;
        assert cb != null;

        int currentPos = this.wrotePosition.get(); // 获取当前MappedFile的写位置

        if (currentPos < this.fileSize) { // 文件还有剩余空间

            // 只有仅当transientStorePoolEnable为true，FlushDiskType为异步刷盘（ASYNC_FLUSH），并且为*_Master时，才启用writeBuffer。

            // writeBuffer/mappedByteBuffer的position始终为0，而limit则等于capacity。
            // slice是根据position和limit来生成byteBuffer。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice(); // @1
            byteBuffer.position(currentPos); // 设置写的起始位置
            AppendMessageResult result = null;
            // 针对不同的消息类型，分别执行不同的追加消息逻辑
            // @2
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes()); // 当前MappedFile对象的当前写指针后移至下一消息写入的位置
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 消息刷盘。
     * <p>
     * 如果MappedFile被shutdown，还会释放堆外内存。
     *
     * @param flushLeastPages 执行刷盘的最少内存页数
     * @return 当前已刷盘的位置（针对每一个MappedFile，offset从0开始）
     */
    public int flush(final int flushLeastPages) {
        // @1
        if (this.isAbleToFlush(flushLeastPages)) { // 校验是否有可刷盘的数据
            if (this.hold()) {
                // @2
                int value = getReadPosition(); // 当前具有可刷盘数据的最大偏移量

                try {
                    // We only append data to fileChannel or mappedByteBuffer, never both.
                    // 我们只将数据附加到fileChannel或mappedByteBuffer，而不是两者。

                    // 只有仅当transientStorePoolEnable为true，FlushDiskType为异步刷盘（ASYNC_FLUSH），并且为*_Master时，才启用writeBuffer。
                    // 从transientStorePool中获取writeBuffer，消息先写入该buffer，然后再写入到fileChannel
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value); // 设置当前刷盘的位置

                // 每次刷盘后，都检查MappedFile是否被shutdown，如果是则会释放堆外内存（mappedByteBuffer）
                this.release(); // @3
            } else {
                // @4
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition()); // 设置当前刷盘的位置
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 将writeBuffer的脏数据写到fileChannel。
     * <p>
     * 如果MappedFile被shutdown，还会释放资源。
     *
     * @param commitLeastPages 执行提交的最少内存页数
     * @return 当前已提交的位置（针对每一个MappedFile，offset从0开始）
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            // no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) { // @1
            if (this.hold()) {
                commit0(commitLeastPages); // @2
                this.release(); // @3
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 如果所有脏数据都已提交给FileChannel，则归还buffer
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) { // 有待提交的数据
            try {
                // 设置提交的范围：position ～ limit
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);

                this.fileChannel.position(lastCommittedPosition); // 设置写入的起始偏移量
                this.fileChannel.write(byteBuffer); // 将writeBuffer偏移量在position ～ limit之间的字节写入fileChannel
                this.committedPosition.set(writePos); // 更新提交的位置
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 校验是否有足够的可刷盘的数据。
     *
     * 只要该MappedFile已经被写满，即wrotePosition等于fileSize，如果已写满则可执行刷盘；
     * 检查尚未刷盘的消息页数是否大于等于最小刷盘页数，页数不够暂时不刷盘；
     *
     * @param flushLeastPages 执行刷盘的最少内存页数
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get(); // 当前MappedFile刷盘的位置
        int write = getReadPosition(); // 获取当前具有可刷盘数据的最大偏移量

        if (this.isFull()) { // 只要该MappedFile已经被写满，即wrotePosition等于fileSize，如果已写满则可执行刷盘；
            return true;
        }

        if (flushLeastPages > 0) { // 检查尚未刷盘的消息页数是否大于等于最小刷盘页数，页数不够暂时不刷盘；
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 校验是否有足够的可提交的数据。
     * <p>
     * 只要该MappedFile已经被写满，即wrotePosition等于fileSize，如果已写满则可执行提交；
     * 检查尚未提交的消息页数是否大于等于最小提交页数，页数不够暂时不提交；
     *
     * @param commitLeastPages 执行提交的最少内存页数
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get(); // 当前MappedFile提交的位置
        int write = this.wrotePosition.get(); // 获取当前具有可提交数据的最大位置

        if (this.isFull()) { // 只要该MappedFile已经被写满，即wrotePosition等于fileSize，如果已写满则可执行提交；
            return true;
        }

        if (commitLeastPages > 0) { // 检查尚未提交的消息页数是否大于等于最小提交页数，页数不够暂时不提交；
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) { // 未shutdown，停止unmapping，返回false
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) { // 已清理，直接返回true
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return 当前具有可刷盘数据的最大偏移量
     */
    public int getReadPosition() {
        // 只有仅当transientStorePoolEnable为true，FlushDiskType为异步刷盘（ASYNC_FLUSH），并且为*_Master时，才启用writeBuffer。
        // 未启用writeBuffer时，则返回当前写入的位置wrotePosition；
        // 启用writeBuffer时，则返回当前提交的位置committedPosition；
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 对当前MappedFile进行预热。
     * <p>
     * 具体的，先对当前MappedFile的每个内存页存入一个字节0，当刷盘策略为同步刷盘时，执行强制刷盘，并且是每修改pages个分页刷一次盘。
     * 然后将当前MappedFile全部的地址空间锁定在物理存储中，防止其被交换到swap空间。
     * 再调用madvise，传入 WILL_NEED 策略，将刚刚锁住的内存预热，其实就是告诉内核，我马上就要用（WILL_NEED）这块内存，先做虚拟内存到物理内存的映射，防止正式使用时产生缺页中断。
     *
     * @param type  刷盘策略
     * @param pages 预热时刷盘时，一次刷盘的分页数
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice(); // @1
        int flush = 0; // 记录上一次刷盘的字节数
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 当刷盘策略为同步刷盘时，执行强制刷盘
            // 每修改pages个分页刷一次盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force(); // @2
                }
            }

            // prevent gc
            // @3
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 将当前MappedFile全部的地址空间锁定在物理存储中，防止其被交换到swap空间。
     * 再调用madvise，传入 WILL_NEED 策略，将刚刚锁住的内存预热，其实就是告诉内核，我马上就要用（WILL_NEED）这块内存，先做虚拟内存到物理内存的映射，防止正式使用时产生缺页中断。
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
