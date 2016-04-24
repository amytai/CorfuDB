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
public class ReplexCommitMsg extends LogUnitMetadataMsg {


    /** The streamID and local offset to write commit bit. */
    UUID streamID;
    long offset;

    public ReplexCommitMsg(UUID streamID, long offset)
    {
        this.msgType = CorfuMsgType.REPLEX_COMMIT;
        this.streamID = streamID;
        this.offset = offset;
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
