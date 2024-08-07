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

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.*;

/**
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 等待创建映射文件的超时时间，默认5秒
     */
    private static int waitTimeOut = 1000 * 5;
    /**
     * 用于保存当前所有待处理的分配请求，其中key是filePath，value是分配请求。
     * <p>
     * 如果分配请求被成功处理，即获取到映射文件，则该请求会从requestTable中移除
     */
    private ConcurrentMap<String, AllocateRequest> requestTable =
            new ConcurrentHashMap<String, AllocateRequest>();
    /**
     * 分配请求队列，注意是优先级队列。
     * <p>
     * 从该队列中获取请求，进而根据请求创建映射文件
     */
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
            new PriorityBlockingQueue<AllocateRequest>();
    /**
     * 标记是否发生异常
     */
    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 提交两个创建映射文件的请求，路径分别为 {@code nextFilePath} 和 {@code nextNextFilePath}，
     * 并等待路径为 {@code nextFilePath} 所对应的映射文件创建完成({@code nextNextFilePath}所对应的映射文件则有服务线程异步创建，并不用等待它创建完)。
     *
     * @param nextFilePath
     * @param nextNextFilePath
     * @param fileSize
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        int canSubmitRequests = 2; // 默认可以提交2个请求
        // 仅当transientStorePoolEnable为true，FlushDiskType为ASYNC_FLUSH，并且broker为主节点时，才启用transientStorePool。
        // 同时在启用快速失败策略时，计算transientStorePool中剩余的buffer数量减去requestQueue中待分配的数量后，剩余的buffer数量，如果数量小于等于0则快速失败。
        // @1^
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) { // @@1
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                    && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { // 如果broker为从节点，那么即使池中没有buffer，也不快速失败（PS：此处的判断是没有意义的，isTransientStorePoolEnable已经限制了broker不能为slave）
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size(); // @@2
            }
        }
        // @1$

        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;
        // 如果requestTable中已存在该路径文件的分配请求，说明该请求已经在排队中，
        // 就不需要再次检查transientStorePool中的buffer是否够用，以及向requestQueue队列中添加分配请求
        // @2^
        if (nextPutOK) {
            if (canSubmitRequests <= 0) { // 如果transientStorePool中的buffer不够了，快速失败
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }
        // @2^

        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        // mmapOperation遇到了异常，先不创建映射文件了
        if (hasException) { // @3
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS); // @4
                if (!waitOK) { // @3
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    // 只负责删除requestTable中的分配请求
                    this.requestTable.remove(nextFilePath); // @6
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMappedFileService.class.getSimpleName();
    }

    public void shutdown() {
        this.stopped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     * 异步处理，调用mmapOperation完成请求的处理。
     */
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 方法只有被外部线程中断，才会返回false。
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            // 检索并删除此队列的首节点，必要时等待，直到有元素可用。
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                // 仅当transientStorePoolEnable为true，FlushDiskType为ASYNC_FLUSH，并且broker为主节点时，才启用transientStorePool
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next(); // @1
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool()); // @2
                    } catch (RuntimeException e) { // 遇到运行异常时用默认配置
                        log.warn("Use default implementation.");
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                // 计算创建映射文件耗时
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }

                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                        .getMapedFileSizeCommitLog()
                        &&
                        this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    // 进行预热
                    // @3
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true; // 标记发生异常
            return false; // 被中断，结束服务线程
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true; // 标记发生异常，但并不会结束服务线程
            if (null != req) {
                requestQueue.offer(req); // 重新加入队列再试
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                req.getCountDownLatch().countDown(); // 唤醒等待获取映射文件的线程
        }
        return true;
    }

    /**
     * 分配请求
     * <p>
     * 内部类，注意实现了{@link Comparable}接口，为了优先队列的插入
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        /**
         * 文件路径
         */
        private String filePath;
        /**
         * 文件大小
         */
        private int fileSize;
        /**
         * 用于实现分配映射文件的等待通知线程模型。
         * <p>
         * 0 代表完成映射文件的创建
         */
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 根据路径以及文件大小创建的映射文件
         */
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }

        /**
         * fileSize 小的 AllocateRequest 对象反而大；
         * fileName 对应的 long 类型大的，AllocateRequest 对象大；
         * <p>
         * 将 AllocateRequest 对象放到优先级队列中，队列的头元素是按该排序方式确定的最小元素。且线程从队列的头部获取元素。
         * 所以 fileSize 大的优先级高；否则，对应 fileName 对应的 long 类型小的，优先级高；
         *
         * @param other
         * @return
         */
        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                // 获取文件名
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
