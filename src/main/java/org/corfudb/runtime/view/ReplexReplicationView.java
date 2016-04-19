package org.corfudb.runtime.view;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.AutoCloseableByteBuf;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** A view of an address implemented by chain replication.
 *
 * What essentially amounts to the CRAQ protocol.
 *
 * Created by amytai on 4/15/16.
 */
@Slf4j
public class ReplexReplicationView extends AbstractReplicationView {

    public ReplexReplicationView(Layout l, Layout.LayoutSegment ls)
    {
        super(l, ls);
    }

    /**
     * Write the given object to an address and streams, using the replication method given.
     *
     * @param address An address to write to.
     * @param stream  The streams which will belong on this entry.
     * @param data    The data to write.
     */
    @Override
    public int write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
    throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        int payloadBytes = 0;
        // To reduce the overhead of serialization, we serialize only the first time we write, saving
        // when we go down the chain.
        try (AutoCloseableByteBuf b =
                     new AutoCloseableByteBuf(ByteBufAllocator.DEFAULT.directBuffer())) {
            Serializers.getSerializer(Serializers.SerializerType.CORFU)
                    .serialize(data, b);
            payloadBytes = b.readableBytes();
                for (int i = 0; i < numUnits; i++)
                {
                    log.trace("Write[{}]: chain {}/{}", address, i+1, numUnits);
                    // In chain replication, we write synchronously to every unit in the chain.
                        CFUtils.getUninterruptibly(
                                getLayout().getLogUnitClient(address, i)
                                        .write(getLayout().getLocalAddress(address), stream, 0L, data, Collections.emptyMap()), OverwriteException.class);
                }
            // TODO: Now we have to write the ACK bits.
        }
        return payloadBytes;
    }

    /**
     * Read the given object from an address, which redirects the read to the logunits with global addresses.
     *
     * @param address The address to read from.
     * @return The result of the read.
     */
    @Override
    public ILogUnitEntry read(long address) {
        // Usually numUnits will be 1, because each server will be replicated once in the Replex Scheme.
        int numUnits = getLayout().getSegmentLength(address);
        log.trace("Read[{}]: chain {}/{}", address, numUnits, numUnits);
        return CFUtils.getUninterruptibly(getLayout()
                        .getLogUnitClient(address, 0).read(getLayout().getLocalAddress(address)))
                            .setAddress(address);
    }

    @Override
    public ILogUnitEntry streamRead(UUID stream, long offset) {
        // Find the correct stripe in the Replex stripelist by hashing the streamID.
        log.trace("StreamRead[{}, {}]", stream, offset);
        return CFUtils.getUninterruptibly(getLayout()
                .getReplexLogUnitClient(stream, 0).read(stream, offset));
    }

    /**
     * Read a stream prefix, using the replication method given.
     *
     * @param stream the stream to read from.
     * @return A map containing the results of the read.
     */
    @Override
    public Map<Long, ILogUnitEntry> read(UUID stream) {
        // TODO: But which replex to read from??
        // for each chain, simply query the last one...
        Set<Map.Entry<Layout.LayoutStripe, Map<Long, LogUnitReadResponseMsg.ReadResult>>> e = segment.getStripes().parallelStream()
                .map(x -> {
                    LogUnitClient luc = layout.getRuntime().getRouter(x.getLogServers().get(x.getLogServers().size() - 1))
                            .getClient(LogUnitClient.class);
                    return new AbstractMap.SimpleImmutableEntry<>(x, CFUtils.getUninterruptibly(luc.readStream(stream)));
                })
                .collect(Collectors.toSet());
        Map<Long, ILogUnitEntry> resultMap = new ConcurrentHashMap<>();

        e.parallelStream()
                .forEach(x ->
                {
                    x.getValue().entrySet().parallelStream()
                            .forEach(y -> {
                                long globalAddress = layout.getGlobalAddress(x.getKey(), y.getKey());
                                y.getValue().setAddress(globalAddress);
                                resultMap.put(globalAddress, y.getValue());
                            });
                });
        return resultMap;
    }

    /**
     * Fill a hole at an address, using the replication method given.
     *
     * @param address The address to hole fill at.
     */
    @Override
    public void fillHole(long address) throws OverwriteException {
        int numUnits = getLayout().getSegmentLength(address);
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}]: chain {}/{}", address, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getLogUnitClient(address, i)
                    .fillHole(address), OverwriteException.class);
        }
        // TODO: Write acks for holes?
        // TODO: WRITE TO REPLEXES AS WELL.
    }

    @Override
    public void fillHole(UUID streamID, long offset) throws OverwriteException {
        int numUnits = getLayout().getStripe(streamID).getLogServers().size();
        for (int i = 0; i < numUnits; i++)
        {
            log.trace("fillHole[{}, {}]: chain {}/{}", streamID, offset, i+1, numUnits);
            // In chain replication, we write synchronously to every unit in the chain.
            CFUtils.getUninterruptibly(getLayout().getReplexLogUnitClient(streamID, i)
                    .fillHole(streamID, offset), OverwriteException.class);
        }
        // TODO: Write acks for holes?
        // TODO: HOW TO FILL HOLES IN REPLEX?
    }
}
