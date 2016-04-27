package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.*;


/**
 * Created by amytai on 4/15/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper=true)
public class ReplexLogUnitWriteMsg extends LogUnitPayloadMsg {


    /** The map reprsents a set of (streamID, local offset) streamPairs that the entry should be written to. */
    Map<UUID, Long> streamPairs;

    public ReplexLogUnitWriteMsg(Map<UUID, Long> streamPairs)
    {
        this.msgType = CorfuMsgType.REPLEX_WRITE;
        this.streamPairs = streamPairs;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }


    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeInt(streamPairs.size());
        for (UUID streamID : streamPairs.keySet()) {
            buffer.writeLong(streamID.getMostSignificantBits());
            buffer.writeLong(streamID.getLeastSignificantBits());
            buffer.writeLong(streamPairs.get(streamID));

        }
    }

    /**
     * Parse the rest of the message from the buffer. Classes that extend CorfuMsg
     * should parse their fields in this method.
     *
     * @param buffer
     */
    @Override
    public void fromBuffer(ByteBuf buffer) {
        super.fromBuffer(buffer);
        int size = buffer.readInt();
        streamPairs = new HashMap<>();
        for (int i = 0; i < size; i++) {
            streamPairs.put(new UUID(buffer.readLong(), buffer.readLong()), buffer.readLong());
        }
    }
}
