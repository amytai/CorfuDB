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
public class LogUnitStreamReadRequestMsg extends CorfuMsg {

    /** Stream address are tuples (streamID, offset), where offset is the local offset within the stream. */
    UUID streamID;
    long offset;

    public LogUnitStreamReadRequestMsg(UUID streamID, long offset)
    {
        this.msgType = CorfuMsgType.READ_REQUEST;
        this.streamID = streamID;
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
