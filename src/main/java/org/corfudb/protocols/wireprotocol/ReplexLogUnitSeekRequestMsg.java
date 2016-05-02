package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;


/**
 * Created by amytai on 4/15/16.
 */
@Getter
@Setter
@NoArgsConstructor
public class ReplexLogUnitSeekRequestMsg extends CorfuMsg {

    /** Find the stream entry in the given stream with a global offset closest to and at least globalAddress */
    long globalAddress;
    UUID streamID;
    long maxLocalOffset;

    public ReplexLogUnitSeekRequestMsg(long globalAddress, UUID streamID, long maxLocalOffset)
    {
        this.msgType = CorfuMsgType.REPLEX_SEEK;
        this.globalAddress = globalAddress;
        this.streamID = streamID;
        this.maxLocalOffset = maxLocalOffset;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(globalAddress);
        buffer.writeLong(streamID.getMostSignificantBits());
        buffer.writeLong(streamID.getLeastSignificantBits());
        buffer.writeLong(maxLocalOffset);
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
        globalAddress = buffer.readLong();
        streamID = new UUID(buffer.readLong(), buffer.readLong());
        maxLocalOffset = buffer.readLong();
    }
}
