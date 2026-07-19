package org.scache.network.ub;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * PREPARE / DATA(chunks) / COMMIT / ABORT Block transfer protocol.
 *
 * 状态机: NEW → PREPARING → TRANSFERRING → COMMITTED (or ABORTED)
 *
 * 只有满足:
 *   completedChunks == chunkCount
 *   实际总长度 == totalLength
 *   CRC(组装后数据) == expectedCRC
 * 才能进入 COMMITTED。
 *
 * ABORTED / PREPARING / TRANSFERRING 状态的 Block 对读取方不可见。
 */
public class BlockProtocol {
    public enum BlockState { NEW, PREPARING, TRANSFERRING, COMMITTED, ABORTED }

    public static class BlockDescriptor {
        public final long blockId, operationId;
        public final int totalLength, chunkSize, chunkCount;
        public final long expectedCRC;
        public final long remoteAddress;
        public final int remoteToken;
        public volatile BlockState state = BlockState.NEW;
        public final boolean[] receivedChunks;
        public final byte[] data;
        public int completedChunks = 0;
        public long committedBytes = 0;
        public String errorReason = null;
        public long createdAt = System.currentTimeMillis();

        BlockDescriptor(long blockId, long operationId, int totalLength, int chunkSize,
                        long expectedCRC, long remoteAddress, int remoteToken) {
            this.blockId = blockId; this.operationId = operationId;
            this.totalLength = totalLength; this.chunkSize = chunkSize;
            this.chunkCount = (totalLength + chunkSize - 1) / chunkSize;
            this.expectedCRC = expectedCRC;
            this.remoteAddress = remoteAddress; this.remoteToken = remoteToken;
            this.receivedChunks = new boolean[chunkCount];
            this.data = new byte[totalLength];
        }
    }

    private final ConcurrentHashMap<Long, BlockDescriptor> blocks = new ConcurrentHashMap<>();
    private final AtomicLong blockIdSeq = new AtomicLong(1);
    private static final int CHUNK_SIZE = 4096;

    /** 准备一个即将接收的 Block。返回 blockId。 */
    public BlockDescriptor prepare(int totalLength, long expectedCRC, long remoteAddress, int remoteToken) {
        long bid = blockIdSeq.getAndIncrement();
        BlockDescriptor bd = new BlockDescriptor(bid, bid, totalLength, CHUNK_SIZE, expectedCRC, remoteAddress, remoteToken);
        bd.state = BlockState.PREPARING;
        blocks.put(bid, bd);
        return bd;
    }

    /** 提交一个已接收的 Chunk 数据。返回 true 表示块已完成。 */
    public synchronized boolean submitChunk(long blockId, int chunkIndex, byte[] chunkData, int offset, int length) {
        BlockDescriptor bd = blocks.get(blockId);
        if (bd == null) return false;
        if (bd.state == BlockState.COMMITTED) return true; // 幂等：已提交完成的 Block 重复提交视为成功
        if (bd.state == BlockState.ABORTED) return false;
        if (chunkIndex < 0 || chunkIndex >= bd.chunkCount) return false;
        if (bd.receivedChunks[chunkIndex]) return true; // 幂等：重复提交视为成功

        bd.state = BlockState.TRANSFERRING;

        int destOffset = chunkIndex * CHUNK_SIZE;
        int copyLen = Math.min(length, bd.totalLength - destOffset);
        System.arraycopy(chunkData, offset, bd.data, destOffset, copyLen);

        bd.receivedChunks[chunkIndex] = true;
        bd.completedChunks++;

        // 检查完成
        if (bd.completedChunks == bd.chunkCount) {
            CRC32 crc = new CRC32();
            crc.update(bd.data);
            if (crc.getValue() == bd.expectedCRC && bd.completedChunks == bd.chunkCount) {
                bd.state = BlockState.COMMITTED;
                bd.committedBytes = bd.totalLength;
                return true;
            } else {
                bd.state = BlockState.ABORTED;
                bd.errorReason = "CRC mismatch or incomplete chunks";
                bd.committedBytes = 0;
                return false;
            }
        }
        return false;
    }

    /** 中止一个 Block。 */
    public void abort(long blockId, String reason) {
        BlockDescriptor bd = blocks.get(blockId);
        if (bd != null && bd.state != BlockState.COMMITTED) {
            bd.state = BlockState.ABORTED;
            bd.errorReason = reason;
            bd.committedBytes = 0;
        }
    }

    /** 获取已提交的 Block 数据（仅 COMMITTED 状态可读）。 */
    public byte[] getCommittedData(long blockId) {
        BlockDescriptor bd = blocks.get(blockId);
        if (bd != null && bd.state == BlockState.COMMITTED) {
            return bd.data;
        }
        return null;
    }

    public BlockState getState(long blockId) {
        BlockDescriptor bd = blocks.get(blockId);
        return bd != null ? bd.state : null;
    }

    public BlockDescriptor getDescriptor(long blockId) {
        return blocks.get(blockId);
    }

    public void removeBlock(long blockId) {
        blocks.remove(blockId);
    }
}
