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
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Store all metadata downtime for recovery, data protection reliability
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    /**
     * final类型，值为 -626843481，MAGIC CODE，表示消息
     */
    public final static int MESSAGE_MAGIC_CODE = -626843481;

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // End of file empty MAGIC CODE cbd43194
    /**
     * final类型，值为 -875286124，MAGIC CODE，表示到达映射文件末尾
     */
    private final static int BLANK_MAGIC_CODE = -875286124;

    /**
     * 映射文件队列，用于管理和维护映射文件
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * 消息存储模块Manager
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 消息刷盘服务，分为同步刷盘和异步刷盘
     */
    private final FlushCommitLogService flushCommitLogService;

    // If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    /**
     * 消息提交服务，
     * <p>
     * 如果启用了TransientStorePool，该服务用于将消息从writeBuffer（堆外内存）提交到fileChannel
     */
    private final FlushCommitLogService commitLogService;

    /**
     * 追加消息回调，
     * <p>
     * 在获取到映射文件后，根据消息格式向映射文件的buffer中写入消息内容
     */
    private final AppendMessageCallback appendMessageCallback;

    /**
     * 追加批量消息时使用，
     * <p>
     * 用于根据消息格式将批量消息封装为ByteBuffer
     */
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;

    /**
     * ConsumeQueue topic-queueid offset table，"topic-queueid"格式为key，offset为value
     */
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    /**
     * 开源版本暂未实现
     */
    private volatile long confirmOffset = -1L;

    /**
     * 消息写入时，会先加锁，然后将其设置为当前时间。
     * 消息写入完成后，先将beginTimeInLock设置为0，然后释放锁。
     * 该值用来计算消息写入耗时。
     * 写入新消息前，会根据该值来检查操作系统内存页写入是否忙，如果上一消息在1s内没有成功写入，则本次消息不再写入，返回页写入忙响应。
     */
    private volatile long beginTimeInLock = 0;

    /**
     * 写入消息时的锁。
     * <p>
     * 有互斥锁ReentrantLock和自旋锁两种锁，默认情况下，使用自旋锁。
     */
    private final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        // 构造mappedFileQueue对象，并指定如下入参:
        //      1. CommitLog文件的存储路径;
        //      2. CommitLog文件大小，默认1G;
        //      3. 映射文件分配线程，RocketMQ使用内存映射处理CommitLog，ConsumeQueue文件
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());

        this.defaultMessageStore = defaultMessageStore;

        // 消息刷盘服务，分为同步刷盘和异步刷盘
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        // 如果启用了transientStorePool，该服务用于将消息从writeBuffer（堆外内存）提交到fileChannel
        this.commitLogService = new CommitRealTimeService();

        // 追加消息回调，
        // 在获取到映射文件后，根据消息格式向映射文件的buffer中写入消息内容
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());

        // 追加批量消息时使用，
        // 用于根据消息格式将批量消息封装为ByteBuffer
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };

        // 确定在存储消息时是否使用互斥锁ReentrantLock。默认情况下，它设置为false，表示在存储消息时使用自旋锁。
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushCommitLogService.start();

        // 如果启用了transientStorePool，我们必须在固定的时间段内将消息刷新到fileChannel
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    /**
     * 获取最后一个映射文件当前具有可刷盘数据的最大物理偏移量
     *
     * @return
     */
    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 根据物理偏移量获取数据。
     * <p>
     * 这里获取的是 {@code offset} 与 {@link MappedFile} 当前具有可刷盘数据的最大物理偏移量之间的数据。
     *
     * @param offset                物理偏移量
     * @param returnFirstOnNotFound 如果MappedFile没有找到，是否返回第一个
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 映射文件的大小，也即CommitLog文件的大小，默认 1G
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        // 根据物理偏移量查找MappedFile
        // 如果returnFirstOnNotFound为true，那么如果MappedFile没有找到，则返回第一个
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize); // 计算映射文件的相对偏移量
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally() {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 解析出单条消息内容，计算{@code tagsCode}，并构造转发请求
     *
     * @param byteBuffer buffer
     * @param checkCRC   是否CRC校验
     * @param readBody   是否读取消息体
     * @return
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE: // 消息
                    break;
                case BLANK_MAGIC_CODE: // 标记到达MappedFile文件末尾
                    return new DispatchRequest(0, true /* success */);
                default: // MAGIC CODE illegal
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong(); // 这是个自增值，不是真正的 consumeQueue 的偏移量（真正的 consumeQueue 的偏移量为 queueOffset * CQ_STORE_UNIT_SIZE），可以代表这个 consumeQueue 或者 tranStateTable 队列中消息的个数。

            long physicOffset = byteBuffer.getLong(); // 消息写入CommitLog的物理偏移量

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    // 读取消息体内容到bytesContent
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    // 消息体循环冗余校验
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen); // 如果不读取消息体，就需要跳过消息体区间
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS); // KEYS

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX); // UNIQ_KEY

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS); // TAGS
                if (tags != null && tags.length() > 0) {
                    // 获取tags的hashcode
                    // TopicFilterType暂时未实现
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                // @@1^
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
                // @@1$
            }

            int readLength = calMsgLength(bodyLen, topicLen, propertiesLength); // 计算消息长度
            if (totalSize != readLength) { // 校验消息长度
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            // 构造转发请求
            return new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //BODYCRC
                + 4 //QUEUEID
                + 4 //FLAG
                + 8 //QUEUEOFFSET
                + 8 //PHYSICALOFFSET
                + 4 //SYSFLAG
                + 8 //BORNTIMESTAMP
                + 8 //BORNHOST
                + 8 //STORETIMESTAMP
                + 8 //STOREHOSTADDRESS
                + 4 //RECONSUMETIMES
                + 8 //Prepared Transaction Offset
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    public void recoverAbnormally() {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                // Normal data
                if (size > 0) {
                    mappedFileOffset += size;

                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                        if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else {
                        this.defaultMessageStore.doDispatch(dispatchRequest);
                    }
                }
                // Intermediate file read error
                else if (size == -1) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // Commitlog case files are deleted
        else {
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // 设置消息存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体循环冗余校验码
        msg.setBodyCRC(UtilAll.crc32(msg.getBody())); // @1
        // 返回结果对象
        AppendMessageResult result = null;

        // 消息存储统计服务
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic(); // topic
        int queueId = msg.getQueueId(); // queue id

        // @2^
        // 获取消息类型
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) { // @@1
            // Delay Delivery
            if (msg.getDelayTimeLevel() > 0) { // 获取消息延时投递时间级别
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) { // @@2
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // topic更改为新的topic SCHEDULE_TOPIC_XXXX
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                // 根据延迟级别获取延时消息的新队列ID（queueId等于延迟级别减去1）
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 将消息中原 topic 和 queueId 存入消息属性中；
                // @@3
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }
        // @2$

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(); // @3

        // 加锁，默认使用自旋锁。
        // 依赖于messageStoreConfig#useReentrantLockWhenPutMessage配置
        putMessageLock.lock(); // spin or ReentrantLock ,depending on store config // @4
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            // 调用MappedFileQueue#getLastMapedFile()方法获取最后一个映射文件，
            // 若还没有映射文件或者已有的最后一个映射文件已经写满则创建一个新的映射文件返回
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise // @5
            }
            if (null == mappedFile) { // 创建映射文件失败
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                // 消息写入完成后，先将beginTimeInLock设置为0，然后释放锁。
                // 该值用来计算消息写入耗时。写入新消息前，会根据该值来检查操作系统内存页写入是否繁忙，如果上一消息在1s内没有成功写入，则本次消息不再写入，返回页写入忙响应。
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 向映射文件中写入消息
            // 注意，这只是将消息写入映射文件中的writeBuffer/mappedByteBuffer，并没有刷盘
            result = mappedFile.appendMessage(msg, this.appendMessageCallback); // @6
            switch (result.getStatus()) {
                case PUT_OK: // 消息成功写入
                    break;
                case END_OF_FILE: // 当前CommitLog可用空间不足
                    unlockMappedFile = mappedFile;
                    // 创建新的CommitLog，重新写入消息
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED: // 消息长度超过了最大阈值
                case PROPERTIES_SIZE_EXCEEDED: // 消息属性长度超过了最大阈值
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR: // 未知错误
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock(); // 释放锁
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        // @7
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 消息统计:
        //      1. 指定topic下写入消息数加1
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        //      2. 指定topic下写入消息的总字节数加上当前写入的字节数
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // 至此，消息并没有真正的写入 CommitLog 文件中，而是在 MappedFile#mappedByteBuffer 或者 MappedFile#writeBuffer 缓存中。
        // 其中，MappedFile#writeBuffer 只有仅当 transientStorePoolEnable 为 true，FlushDiskType 为异步刷盘（ASYNC_FLUSH），并且 broker 为主节点时才启用。

        handleDiskFlush(result, putMessageResult, msg); // @8
        handleHA(result, putMessageResult, msg); // @9

        return putMessageResult;
    }

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {

            // 同步刷盘策略在初始化时，初始化的是GroupCommitService实例
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;

            if (messageExt.isWaitStoreMsgOK()) { // 1.「同步阻塞」等待消息刷盘完成。线程会阻塞，直至收到消息刷盘完成的通知；@1

                // 创建同步阻塞刷盘请求
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());  // @2
                // 添加刷盘请求，并唤醒刷盘服务线程
                service.putRequest(request); // @3

                // 因为是同步阻塞刷盘，所以需要等待完成刷盘的通知
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout()); // @4
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                            + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else { // 2.「同步非阻塞」不等待消息刷盘完成。线程会继续执行；
                service.wakeup(); // 唤醒刷盘服务线程
            }
        }
        // 异步刷盘
        else {
            // 仅当transientStorePoolEnable为true，FlushDiskType为异步刷盘（ASYNC_FLUSH），并且broker为主节点时，才启用writeBuffer。
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                // 1.未启用transientStorePool
                // 异步刷盘策略在初始化时，初始化的是FlushRealTimeService实例
                flushCommitLogService.wakeup(); // @1
            } else {
                // 2.启用transientStorePool
                // 启用writeBuffer时，需要提交数据到fileChannel，然后再刷盘
                commitLogService.wakeup(); // @2
            }
        }
    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 高可用是由Master处理的
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    boolean flushOK =
                            request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                                + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        // 设置消息存储时间
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        // 返回结果对象
        AppendMessageResult result;

        // 消息存储统计服务
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 获取消息类型
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        // 批量消息写入不支持事务消息和延迟消息
        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get(); // @1

        // 将批量消息封装为ByteBuffer，并保存到messageExtBatch的encodedBuff中
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        // 加锁，默认使用自旋锁。
        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            // 调用MappedFileQueue#getLastMapedFile()方法获取最后一个映射文件，
            // 若还没有映射文件或者已有的最后一个映射文件已经写满则创建一个新的映射文件返回
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 向映射文件中写入消息
            // 注意，这只是将消息写入映射文件中的writeBuffer/mappedByteBuffer，并没有刷盘
            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock(); // 释放锁
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, messageExtBatch.getBody().length, result);
        }

        // 由于当前CommitLog可用空间不足，则会创建新的CommitLog，而当前的CommitLog所对应的MappedFile中的mappedByteBuffer如果启用预热，则需要执行munlock，使其可以被OS swap到磁盘。
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 消息统计:
        //      1. 指定topic下写入消息数累加msgNum
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        //      2. 指定topic下写入消息的总字节数加上当前写入的字节数
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        // 刷盘
        handleDiskFlush(result, putMessageResult, messageExtBatch);

        // 主从同步
        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 异步提交服务线程
     */
    class CommitRealTimeService extends FlushCommitLogService {

        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 提交数据到fileChannel的超时等待时间，默认200毫秒
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                // 提交数据到fileChannel时所提交的最少内存页数，默认4
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                // @1
                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) { // result为false，意味着有新的数据提交，则需要刷盘
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        // now wake up flush thread.
                        // 唤醒刷盘线程
                        flushCommitLogService.wakeup(); // @2
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }

                    this.waitForRunning(interval); // @3
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            // @4
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 异步刷盘服务线程
     *
     * 有实时刷盘和定时刷盘两种策略，默认为实时刷盘
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0; // 最后一次刷盘的时间
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                // 是否是定时刷盘。默认是实时刷盘
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                // 1. 实时刷盘的超时等待时间;
                // 2. 定时刷盘的间隔时间；
                // 默认500毫秒
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();

                // 刷盘的最少内存页数，默认4
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                // @1^
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }
                // @1$

                try {
                    if (flushCommitLogTimed) { // 定时刷盘
                        Thread.sleep(interval);
                    } else { // 实时刷盘，依赖CommitRealTimeService唤醒、等待超时或者中断
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);

                    // 采用完全刷盘方式（flushLeastPages为0）时，所刷盘的最后一条消息存储的时间戳
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            // 正常关闭服务，确保退出前全部刷盘
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        /**
         * 可刷盘数据的最大位置
         */
        private final long nextOffset;
        /**
         * 用于实现创建刷盘请求的线程的等待通知，初始值为 1。
         *
         * 这不同于刷盘服务线程的等待通知，刷盘服务线程的等待通知是由{@link ServiceThread}提供支持的
         */
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        /**
         * 标记刷盘是否成功
         */
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        /**
         * 刷盘服务线程在完成刷盘后会调用该方法唤醒当前等待的线程
         *
         * @param flushOK 刷盘是否成功
         */
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            // 刷盘完成，唤醒当前等待的线程
            this.countDownLatch.countDown();
        }

        /**
         * 等待刷盘完成的通知。
         *
         * 注意，只有在成功刷盘时才会返回true，被中断或者等待超时都会返回false。
         *
         * @param timeout
         * @return
         */
        public boolean waitForFlush(long timeout) {
            try {
                // 等待异步线程刷盘完成的通知
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * 同步刷盘服务线程
     */
    class GroupCommitService extends FlushCommitLogService {
        /**
         * 刷盘请求列表
         */
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        /**
         * 刷盘请求列表
         */
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * 添加刷盘请求，并唤醒刷盘服务线程
         *
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request); // 添加刷盘请求
            }
            // 有新的刷盘请求，如果当前没有其它的刷盘请求通知刷盘服务线程，则通知该刷盘服务线程（将其唤醒）；
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // 通知刷盘服务线程，将其唤醒
            }
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) { // 同步阻塞刷盘
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        // 执行两次刷盘，可能因为当前待刷盘的CommitLog空间不足，当前待刷盘的消息被写到了下一个MappedFile
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset(); // @@1

                            if (!flushOK) {
                                CommitLog.this.mappedFileQueue.flush(0); // @@2
                            }
                        }

                        req.wakeupCustomer(flushOK); // @@3
                    }

                    // 采用完全刷盘方式（flushLeastPages为0）时，所刷盘的最后一条消息存储的时间戳
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear(); // 清空读请求队列
                } else { // 同步非阻塞刷盘
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0); // @@4
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10); // @1
                    this.doCommit(); // @2
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            // 在正常情况下停止服务，会等待请求到达，然后刷盘

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        /**
         * 至少预留8个字节，用以标记映射文件写满
         */
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        /**
         * 临时保存消息ID的buffer，默认16字节。
         * <p>
         * 前4字节为broker存储地址的host，5～8字节为broker存储地址的port，最后8字节为wroteOffset
         */
        private final ByteBuffer msgIdMemory;

        // Store the message content
        /**
         * 临时保存单个消息的buffer，默认4M。
         * <p>
         * 此外还会多分配 8 个字节，用于在映射文件写满时写入标记位。
         */
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        /**
         * 消息长度的最大阈值，默认4M。
         * <p>
         * 在批量插入时，还会用于限制批量消息的总大小。
         */
        private final int maxMessageSize;
        // Build Message Key
        /**
         * 构建消息key，格式为"topic-queueId"
         */
        private final StringBuilder keyBuilder = new StringBuilder();

        /**
         * 构建msgId。
         *
         * 用于处理批量消息时调用，以逗号作为分割符将批量消息中的所有消息ID拼接。
         */
        private final StringBuilder msgIdBuilder = new StringBuilder();

        /**
         * 从消息的{@link MessageExt#storeHost}中获取host保存到{@link #hostHolder}的前4个字节，port保存到后4个字节
         */
        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        /**
         * 该构造函数在CommitLog构造函数中调用，用来构造DefaultAppendMessageCallback对象
         *
         * @param size 允许的最大消息长度，默认4M
         */
        DefaultAppendMessageCallback(final int size) {
            // 分配临时保存消息ID的buffer，默认16字节，前4字节为host，5～8字节为port，最后8字节为wroteOffset
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            // 分配临时保存单个消息的buffer，默认4M。
            // 此外还会多分配 8 个字节，用于在映射文件写满时写入标记位。
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            // 消息长度的最大阈值，默认4M。在批量插入时，还会用于限制批量消息的总大小。
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 追加单条消息
         *
         * @param fileFromOffset CommitLog文件的起始偏移量。其实就是文件名称，一般为20位数字
         * @param byteBuffer     写消息缓冲区（{@link MappedFile#writeBuffer} 或者 {@link MappedFile#mappedByteBuffer}）
         * @param maxBlank       写消息缓冲区剩余可用空间大小
         * @param msgInner       单条消息
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner) {

            // 消息写入的物理偏移量（CommitLog文件（对应一个MappedFile）对应的起始偏移量 + 当前映射文件的写位置）
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 将hostHolder重置，使其可以再次使用
            this.resetByteBuffer(hostHolder, 8); // @1
            // 生成消息ID，前4字节为broker存储地址的host，5～8字节为broker存储地址的port，最后8字节为wroteOffset
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset); // @2

            // Record ConsumeQueue information
            keyBuilder.setLength(0); // @3
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            // propertiesString bytes
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            // propertiesString bytes length
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            // topic bytes
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            // topic bytes length
            final int topicLength = topicData.length;

            // body bytes length
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 计算消息长度，即写入CommitLog占用的空间
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength); // @4

            // Exceeds the maximum message
            // 消息长度超过了最大阈值
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            // 确定是否有足够的可用空间
            // @5
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) { // 没有足够的可用空间
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank); // 剩余可用空间的总长度
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE); // 标记CommitLog文件结尾
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                // 返回END_OF_FILE响应
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen); // 代表消息的大小
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE); // 标记消息
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC()); // 消息体循环冗余校验码
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId()); // 消息队列ID
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag()); // 标记位
            // 6 QUEUEOFFSET
            // 这是个自增值，不是真正的 consumeQueue 的偏移量（真正的 consumeQueue 的偏移量为 queueOffset * CQ_STORE_UNIT_SIZE），
            // 可以代表这个 consumeQueue 或者 tranStateTable 队列中消息的个数。
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position()); // 消息写入的物理偏移量（CommitLog文件（对应一个MappedFile）对应的起始偏移量 + 当前映射文件的写位置）
            // 8 SYSFLAG
            /*
                指明消息是否是事务消息，以及事务状态等消息特征，二进制表示形式为四个字节，
                从右往左数：
                    仅当 4 个字节均为 0（值为 0）时表示非事务消息（TRANSACTION_NOT_TYPE）；
                    仅当第 1 个字节为 1（值为 1）时表示表示消息是压缩的（COMPRESSED_FLAG）；
                    仅当第 2 个字节为 1（值为 2）表示 multi tags 消息（MULTI_TAGS_FLAG）；
                    仅当第 3 个字节为 1（值为 4）时表示 prepared 消息（TRANSACTION_PREPARED_TYPE）；
                    仅当第 4 个字节为 1（值为 8）时表示 commit 消息（TRANSACTION_COMMIT_TYPE）；
                    仅当第 3，4 个字节均为 1 时（值为 12）时表示 rollback 消息（TRANSACTION_ROLLBACK_TYPE）。
                    此外，只要第 3，4 个字节都为 0 时表示非事务消息。
             */
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp()); // 客户端创建消息的时间戳
            // 10 BORNHOST
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder)); // 客户端地址，前4字节为host，后4字节为port
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp()); // 消息在broker存储的时间戳
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder)); // 存储在broker的地址，前4字节为host，后4字节为port
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes()); // 消息被某个订阅组重新消费了几次（订阅组之间独立计数），因为重试消息发送到了topic为%retry%groupName、queueId=0的队列中去了，成功消费一次记录为0；
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset()); // 表示是prepared状态的事务消息
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength); // 消息体大小
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody()); // 消息体内容
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength); // topic大小
            this.msgStoreItemMemory.put(topicData); // topic内容
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength); // 消息属性大小
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData); // 消息属性内容

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            /*
                AppendMessageResult响应相关字段：
                       AppendMessageStatus status   响应状态码,
                       long wroteOffset             消息写入的物理偏移量（CommitLog文件（对应一个MappedFile）对应的起始偏移量 + 当前消息写入时的写位置）。注意，是当前消息写入时的写位置，当前消息写入后会更新，
                       int wroteBytes               消息写入总字节数。注意，有可能会同时写入多条消息，所以这里是写入的总字节数,
                       String msgId                 消息ID，前4字节为broker存储地址的host，5～8字节为broker存储地址的port，最后8字节为wroteOffset,
                       long storeTimestamp          消息存储时间戳,
                       long logicsOffset            这是个自增值，不是真正的 consumeQueue 的偏移量（真正的 consumeQueue 的偏移量为 queueOffset * CQ_STORE_UNIT_SIZE），可以代表这个 consumeQueue 或者 tranStateTable 队列中消息的个数,
                       long pagecacheRT             消息写入PageCache花费的时间
             */
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            // @6
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        /**
         * 追加批量消息
         *
         * @param fileFromOffset  CommitLog文件的起始偏移量。其实就是文件名称，一般为20位数字
         * @param byteBuffer      写消息缓冲区（{@link MappedFile#writeBuffer} 或者 {@link MappedFile#mappedByteBuffer}）
         * @param maxBlank        写消息缓冲区剩余可用空间大小
         * @param messageExtBatch 批量消息
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch) {
            // 用于CommitLog空间不足追加消息失败时回滚
            byteBuffer.mark();
            // physical offset
            // 消息写入的物理偏移量（CommitLog文件（对应一个MappedFile）对应的起始偏移量 + 当前映射文件的写位置）
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0; // 批量消息中所有消息的总长度
            int msgNum = 0; // 批量消息中消息的总数
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();

            // 获取批量消息的字节缓冲区
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            this.resetByteBuffer(hostHolder, 8);
            // 从消息的storeHost中获取host保存到hostHolder的前4个字节，port保存到后4个字节
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(hostHolder);

            // 用于CommitLog空间不足追加消息失败时回滚
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt(); // 单条消息的长度
                final int bodyLen = msgLen - 40; // only for log, just estimate it
                // Exceeds the maximum message
                // 消息长度超过了最大阈值
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                // 确定是否有足够的可用空间
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value

                    // ignore previous read
                    // 回滚，忽略之前的读取
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    // 回滚，忽略之前追加的消息
                    byteBuffer.reset(); // ignore the previous appended messages

                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }

                // move to add queue offset and commitlog offset
                // 设置 QUEUEOFFSET 和 PHYSICALOFFSET
                // 之所以右移，这是由消息的存储格式决定的，详细参见MessageExtBatchEncoder#encode
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind(); // @1
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            // 消息写入buffer
            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);

            messageExtBatch.setEncodedBuff(null);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        /**
         * 将buffer重置，使其可以再次使用。
         * <p>
         * 具体的，将limit、position、mark设置为初始值。
         *
         * @param byteBuffer 待重置的buffer
         * @param limit
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {
        /**
         * 保存批量消息内容的buffer
         */
        private final ByteBuffer msgBatchMemory;
        /**
         * 消息长度的最大阈值，默认4M。
         *
         * 同时还会用于限制批量消息的总大小
         */
        private final int maxMessageSize;

        /**
         * 保存地址的buffer
         */
        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            // 重置msgBatchMemory
            msgBatchMemory.clear(); // not thread-safe // @1
            // 保存当前遍历的所有消息的总长度
            int totalMsgLen = 0;
            // 将消息的消息体二进制字节数组包装为ByteBuffer
            ByteBuffer messagesByteBuff = messageExtBatch.wrap(); // @2
            while (messagesByteBuff.hasRemaining()) {
                // @3^
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                // 获取消息体，并获取循环冗余校验码
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);
                // @3$

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(bodyLen, topicLength, propertiesLen);

                // 消息长度超过了最大阈值
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                            + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;

                // 批量消息总长度超过了最大阈值
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0); // Set when executing the doAppend method
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0); // Set when executing the doAppend method
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(hostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
