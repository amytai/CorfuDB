package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import javafx.util.Pair;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResult;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReplexSeekResult;
import org.corfudb.runtime.exceptions.OutOfSpaceException;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** A client to a ReplexLogUnit.
 *
 * This class provides access to operations on a remote replex log unit. For now, replexes are log units that
 * are indexed by (streamID, offset).
 * Created by amytai on 4/15/16.
 */
public class ReplexLogUnitClient implements IClient {
    @Setter
    IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case ERROR_OK:
                router.completeRequest(msg.getRequestID(), true);
                break;
            case ERROR_TRIMMED:
                router.completeExceptionally(msg.getRequestID(), new Exception("Trimmed"));
                break;
            case ERROR_OVERWRITE:
                router.completeExceptionally(msg.getRequestID(), new OverwriteException());
                break;
            case ERROR_OOS:
                router.completeExceptionally(msg.getRequestID(), new OutOfSpaceException());
                break;
            case ERROR_RANK:
                router.completeExceptionally(msg.getRequestID(), new Exception("Rank"));
                break;
            case READ_RESPONSE:
                router.completeRequest(msg.getRequestID(), new ReadResult((LogUnitReadResponseMsg)msg));
                break;
            /*case READ_RANGE_RESPONSE: {
                LogUnitReadRangeResponseMsg rmsg = (LogUnitReadRangeResponseMsg) msg;
                Map<Long, ReadResult> lr = new ConcurrentHashMap<>();
                rmsg.getResponseMap().entrySet().parallelStream()
                    .forEach(e -> lr.put(e.getKey(), new ReadResult(e.getValue())));
                router.completeRequest(msg.getRequestID(), lr);
            }
            break;
            case CONTIGUOUS_TAIL: {
                LogUnitTailMsg m = (LogUnitTailMsg) msg;
                router.completeRequest(msg.getRequestID(), new ContiguousTailData(m.getContiguousTail(),
                        m.getStreamAddresses()));
            }
            break;*/
            case REPLEX_READ_RESPONSE: {
                ReplexLogUnitReadResponseMsg m = (ReplexLogUnitReadResponseMsg) msg;
                ReadResult rr = new ReadResult(m);
                rr.setAddress(m.getGlobalAddress());
                router.completeRequest(msg.getRequestID(), rr);
            }
            break;
            case REPLEX_SEEK_RESPONSE: {
                ReplexLogUnitSeekResponseMsg m = (ReplexLogUnitSeekResponseMsg) msg;
                ReplexSeekResult sr = new ReplexSeekResult(m);
                sr.setAddress(m.getGlobalAddress());
                router.completeRequest(msg.getRequestID(), sr);
            }
            break;
            case REPLEX_READ_RANGE_RESPONSE: {
                ReplexLogUnitReadRangeResponseMsg m = (ReplexLogUnitReadRangeResponseMsg) msg;
                Map<Pair<UUID, Long>, ReadResult> map = new ConcurrentHashMap<>();
                m.getResponseMap().entrySet().parallelStream()
                        .forEach(e -> {
                            ReadResult rr = new ReadResult(e.getValue());
                            rr.setAddress(e.getValue().getGlobalAddress());
                            map.put(e.getKey(), rr);
                        });
                router.completeRequest(msg.getRequestID(), map);
            }
            break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<CorfuMsg.CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.CorfuMsgType>()
                    .add(CorfuMsg.CorfuMsgType.WRITE)
                    .add(CorfuMsg.CorfuMsgType.READ_REQUEST)
                    .add(CorfuMsg.CorfuMsgType.READ_RESPONSE)
                    .add(CorfuMsg.CorfuMsgType.TRIM)
                    .add(CorfuMsg.CorfuMsgType.FILL_HOLE)
                   /* .add(CorfuMsg.CorfuMsgType.FORCE_GC)
                    .add(CorfuMsg.CorfuMsgType.GC_INTERVAL)
                    .add(CorfuMsg.CorfuMsgType.FORCE_COMPACT)
                    .add(CorfuMsg.CorfuMsgType.CONTIGUOUS_TAIL)
                    .add(CorfuMsg.CorfuMsgType.GET_CONTIGUOUS_TAIL)*/
                    .add(CorfuMsg.CorfuMsgType.READ_RANGE)
                    .add(CorfuMsg.CorfuMsgType.READ_RANGE_RESPONSE)
                    .add(CorfuMsg.CorfuMsgType.STREAM_READ)
                    .add(CorfuMsg.CorfuMsgType.REPLEX_READ_RESPONSE)
                    .add(CorfuMsg.CorfuMsgType.REPLEX_SEEK_RESPONSE)
                    .add(CorfuMsg.CorfuMsgType.REPLEX_READ_RANGE_RESPONSE)

                    .add(CorfuMsg.CorfuMsgType.ERROR_OK)
                    .add(CorfuMsg.CorfuMsgType.ERROR_TRIMMED)
                    .add(CorfuMsg.CorfuMsgType.ERROR_OVERWRITE)
                    .add(CorfuMsg.CorfuMsgType.ERROR_OOS)
                    .add(CorfuMsg.CorfuMsgType.ERROR_RANK)
                    .build();

    /**
     * Asynchronously write to the logging unit.
     *
     * It is assumed that the calling function ensures that all streams in streamPairs.keySet() hash to the log unit
     * connected to this client.
     *
     * @param streamPairs       Map of stream address that this entry should be written to.
     * @param rank              The rank of this write (used for quorum replication).
     * @param writeObject       The object, pre-serialization, to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(Map<UUID, Long> streamPairs, long globalAddress, long rank,
                                            Object writeObject)
    {
        ReplexLogUnitWriteMsg w = new ReplexLogUnitWriteMsg(streamPairs, globalAddress);
        w.setRank(rank);
        w.setPayload(writeObject);
        return router.sendMessageAndGetCompletable(w);
    }

    /**
     * Asynchronously write to the logging unit.
     *
     * @param address           The address to write to.
     * @param streams           The streams, if any, that this write belongs to.
     * @param rank              The rank of this write (used for quorum replication).
     * @param buffer            The object, post-serialization, to write.
     * @param backpointerMap    The map of backpointers to write.
     * @return A CompletableFuture which will complete with the WriteResult once the
     * write completes.
     */
    public CompletableFuture<Boolean> write(long address, Set<UUID> streams, long rank,
                                            ByteBuf buffer, Map<UUID,Long> backpointerMap)
    {
        LogUnitWriteMsg w = new LogUnitWriteMsg(address);
        w.setStreams(streams);
        w.setRank(rank);
        w.setBackpointerMap(backpointerMap);
        w.setData(buffer);
        return router.sendMessageAndGetCompletable(w);
    }

    public CompletableFuture<Boolean> writeCommit(Map<UUID, Long> streamPairs, boolean commit)
    {
        ReplexCommitMsg m = new ReplexCommitMsg(streamPairs);
        m.setReplexCommit(commit);
        return router.sendMessageAndGetCompletable(m);
    }


    /**
     * Asynchronously read from the logging unit.
     *
     * @param streamID  the stream to read from.
     * @param offset    the offset within the stream to read at.
     * @return A CompletableFuture which will complete with a ReadResult once the read
     * completes.
     */
    public CompletableFuture<ReadResult> read(UUID streamID, long offset) {
        return router.sendMessageAndGetCompletable(new ReplexLogUnitReadRequestMsg(streamID, offset));
    }

    /* Returns the LogEntry in streamID that has a global offset at least as large as and closest to globalAddress*/
    public CompletableFuture<ReplexSeekResult> seek(long globalAddress, UUID streamID, long maxLocalOffset) {
        return router.sendMessageAndGetCompletable(
                new ReplexLogUnitSeekRequestMsg(globalAddress, streamID, maxLocalOffset));
    }

    /**
     * Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream The stream to trim.
     * @param prefix The prefix of the stream, as a global physical offset, to trim.
     */
    public void trim(UUID stream, long prefix) {

        router.sendMessage(new LogUnitTrimMsg(prefix, stream));
    }

    /**
     * Fill a hole at a given address.
     *
     * @param address The address to fill a hole at.
     */
    public CompletableFuture<Boolean> fillHole(long address) {
        return router.sendMessageAndGetCompletable(new LogUnitFillHoleMsg(address));
    }

    public CompletableFuture<Boolean> fillHole(UUID streamID, long offset) {
        return router.sendMessageAndGetCompletable(new ReplexLogUnitFillHoleMsg(streamID, offset));
    }

    /**
     * Force the garbage collector to begin garbage collection.
     */
    public void forceGC() {
        router.sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.FORCE_GC));
    }

    /**
     * Force the compactor to recalculate the contiguous tail.
     */
    public void forceCompact() {
        router.sendMessage(new CorfuMsg(CorfuMsg.CorfuMsgType.FORCE_COMPACT));
    }

    /**
     * Read a range of addresses.
     *
     * @param addresses The addresses to read.
     */
    public CompletableFuture<Map<Pair<UUID, Long>,ReadResult>> readRange(Map<UUID, RangeSet<Long>> addresses) {
        return router.sendMessageAndGetCompletable(new ReplexLogUnitReadRangeRequestMsg(addresses));
    }

    /**
     * Read a contiguous stream prefix
     *
     * @param streamID The stream to read.
     */
    public CompletableFuture<Map<Long,ReadResult>> readStream(UUID streamID) {
        return router.sendMessageAndGetCompletable(new CorfuUUIDMsg(CorfuMsg.CorfuMsgType.STREAM_READ, streamID));
    }


    @Data
    public static class ContiguousTailData {
        final Long contiguousTail;
        final RangeSet<Long> range;
    }

    /** Get the contiguous tail data for a particular stream.
     *
     * @param stream    The contiguous tail for a stream.
     * @return          A ContiguousTailData containing the data for that stream.
     */
    public CompletableFuture<ContiguousTailData> getContiguousTail(UUID stream)
    {
       return router.sendMessageAndGetCompletable(new CorfuUUIDMsg(CorfuMsg.CorfuMsgType.GET_CONTIGUOUS_TAIL, stream));
    }

    /**
     * Change the default garbage collection interval.
     *
     * @param millis    The new garbage collection interval, in milliseconds.
     */
    public void setGCInterval(long millis) {
        router.sendMessage(new LogUnitGCIntervalMsg(millis));
    }

}