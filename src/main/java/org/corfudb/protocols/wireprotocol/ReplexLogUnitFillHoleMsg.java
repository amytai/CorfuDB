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
public class ReplexLogUnitFillHoleMsg extends CorfuMsg {


    /** The Replex address to fill the hole at. */
    UUID streamID;
    long offset;

    public ReplexLogUnitFillHoleMsg(UUID streamID, long offset)
    {
        this.msgType = CorfuMsgType.FILL_HOLE;
        this.streamID  = streamID;
        this.offset = offset;
    }
    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeLong(streamID.getMostSignificantBits());
        buffer.writeLong(streamID.getLeastSignificantBits());
        buffer.writeLong(offset);
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
        streamID = new UUID(buffer.readLong(), buffer.readLong());
        offset = buffer.readLong();
    }
}
