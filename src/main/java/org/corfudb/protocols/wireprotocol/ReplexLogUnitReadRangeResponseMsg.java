package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import javafx.util.Pair;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by amytai on 5/4/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class ReplexLogUnitReadRangeResponseMsg extends CorfuMsg {

    Map<Pair<UUID, Long>, ReplexLogUnitReadResponseMsg> responseMap;

    public ReplexLogUnitReadRangeResponseMsg(Map<Pair<UUID, Long>, ReplexLogUnitReadResponseMsg> map)
    {
        this.msgType = CorfuMsgType.REPLEX_READ_RANGE_RESPONSE;
        this.responseMap = map;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeInt(responseMap.size());
        for (Map.Entry<Pair<UUID, Long>,ReplexLogUnitReadResponseMsg> e : responseMap.entrySet())
        {
            buffer.writeLong(e.getKey().getKey().getMostSignificantBits());
            buffer.writeLong(e.getKey().getKey().getLeastSignificantBits());
            buffer.writeLong(e.getKey().getValue());
            e.getValue().serialize(buffer);
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
        int size = buffer.readInt();
        responseMap = new HashMap<>();
        for (int i = 0; i < size; i++)
        {
            Pair<UUID, Long> streamAddress = new Pair(
                    new UUID(buffer.readLong(), buffer.readLong()), buffer.readLong());
            ReplexLogUnitReadResponseMsg m = (ReplexLogUnitReadResponseMsg)(CorfuMsg.deserialize(buffer));
            responseMap.put(streamAddress, m);
        }
    }
}
