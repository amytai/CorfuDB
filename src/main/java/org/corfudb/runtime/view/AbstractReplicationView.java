package org.corfudb.runtime.view;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/** All replication views must inherit from this class.
 *
 * This class takes a layout as a constructor and provides an address space with
 * the correct replication view given a layout and mode.
 *
 * Created by mwei on 12/11/15.
 */
@Slf4j
public abstract class AbstractReplicationView {

    public static AbstractReplicationView getReplicationView(Layout l, Layout.ReplicationMode mode, Layout.LayoutSegment ls) {
        switch (mode) {
            case CHAIN_REPLICATION:
                return new ChainReplicationView(l, ls);
            case QUORUM_REPLICATION:
                log.warn("Quorum replication is not yet supported!");
                break;
            case REPLEX_REPLICATION:
                return new ReplexReplicationView(l, ls);
        }
        log.error("Unknown replication mode {} selected.", mode);
        throw new RuntimeException("Unsupported replication mode.");
    }

    @ToString(exclude = {"runtime"})
    @RequiredArgsConstructor
    public static class CachedLogUnitEntry implements ILogUnitEntry {
        @Getter
        final LogUnitReadResponseMsg.ReadResultType resultType;

        @Getter
        final EnumMap<LogUnitMetadataType, Object> metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);

        @Getter
        final Object payload;

        @Getter
        final Long address;

        final CorfuRuntime runtime;

        public ILogUnitEntry setRuntime(CorfuRuntime runtime) {
            return this;
        }

        @Getter
        final int sizeEstimate;

        /**
         * Gets a ByteBuf representing the payload for this data.
         *
         * @return A ByteBuf representing the payload for this data.
         */
        @Override
        public ByteBuf getBuffer() {
            log.warn("Attempted to get a buffer of a cached entry!");
            throw new RuntimeException("Invalid attempt to get the ByteBuf of a cached entry!");
        }
    }

    @Getter
    public final Layout layout;

    @Getter
    public final Layout.LayoutSegment segment;

    public AbstractReplicationView(Layout layout, Layout.LayoutSegment ls) {
        this.layout = layout;
        this.segment = ls;
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    public void write(long address, Set<UUID> stream, Object data)
            throws OverwriteException {
        write(address, stream, data, Collections.emptyMap());
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address        An address to write to.
     * @param stream         The streams which will belong on this entry.
     * @param data           The data to write.
     * @param backpointerMap The map of backpointers to write.
     * @return The number of bytes that was remotely written.
     */
    public abstract int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
            throws OverwriteException;

    /**
     * Read the given object from an address, using the replication method given.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    public abstract ILogUnitEntry read(long address);

    public ILogUnitEntry streamRead(UUID stream, long offset) {
        throw new UnsupportedOperationException("Tried streamRead on a ReplicationView that doesn't support");
    }

    /**
     * Read a set of addresses, using the replication method given.
     *
     * @param addresses The addresses to read from.
     * @return A map containing the results of the read.
     */
    public Map<Long, ILogUnitEntry> read(RangeSet<Long> addresses) {
        Map<Long, ILogUnitEntry> results = new ConcurrentHashMap<>();
        Set<Long> total = Utils.discretizeRangeSet(addresses);
        total.parallelStream()
                .forEach(i -> results.put(i, read(i)));
        return results;
    }

    /**
     * Read a contiguous stream prefix, using the replication method given.
     *
     * @param stream The stream to read from.
     * @return A map containing the results of the read.
     */
    public abstract Map<Long, ILogUnitEntry> read(UUID stream);

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    public abstract void fillHole(long address)
            throws OverwriteException;

    public void fillHole(UUID streamID, long offset) throws OverwriteException {
        throw new UnsupportedOperationException("This log unit doesn't implement fillStreamHole");
    }

}
