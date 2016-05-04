package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import javafx.util.Pair;
import lombok.*;
import org.corfudb.util.serializer.Serializers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Created by amytai on 5/4/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class ReplexLogUnitReadRangeRequestMsg extends CorfuMsg {

    Map<UUID, RangeSet<Long>> ranges;

    public ReplexLogUnitReadRangeRequestMsg(Map<UUID, RangeSet<Long>> ranges)
    {
        this.msgType = CorfuMsgType.REPLEX_READ_RANGE_REQUEST;
        this.ranges = ranges;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeInt(ranges.size());
        for (UUID streamID : ranges.keySet())
        {
            buffer.writeLong(streamID.getMostSignificantBits());
            buffer.writeLong(streamID.getLeastSignificantBits());

            Set<Range<Long>> toSerialize = ranges.get(streamID).asRanges();
            buffer.writeInt(toSerialize.size());
            for (Range i : toSerialize) {
                Serializers.getSerializer(Serializers.SerializerType.JAVA).serialize(i, buffer);
            }
        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    @SuppressWarnings("unchecked")
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        this.ranges = new HashMap<>();
        int numStreams = buffer.readInt();
        for (int i = 0; i < numStreams; i++)
        {
            UUID stream = new UUID(buffer.readLong(), buffer.readLong());
            int sizeOfRangeSet = buffer.readInt();

            RangeSet<Long> curSet = TreeRangeSet.create();
            for (int j = 0; j < sizeOfRangeSet; j++) {
                Range r = (Range) Serializers.getSerializer(Serializers.SerializerType.JAVA).deserialize(buffer, null);
                curSet.add(r);
            }
            this.ranges.put(stream, curSet);
        }
    }
}
