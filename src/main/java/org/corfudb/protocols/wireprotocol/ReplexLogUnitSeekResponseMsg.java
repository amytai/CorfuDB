package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.*;
import lombok.experimental.Accessors;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.*;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Created by amytai on 5/1/16.
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class ReplexLogUnitSeekResponseMsg extends ReplexLogUnitReadResponseMsg {

    public long streamOffset; // The local offset of the returned entry within the requested stream.

    public ReplexLogUnitSeekResponseMsg(ReadResultType result, long streamOffset, long globalAddress)
    {
        super(result, globalAddress);
        this.msgType = CorfuMsgType.REPLEX_SEEK_RESPONSE;
        this.streamOffset = streamOffset;
    }

    public ReplexLogUnitSeekResponseMsg(LogUnitEntry entry, long streamOffset, long globalAddress)
    {
        super(entry, globalAddress);
        this.msgType = CorfuMsgType.REPLEX_SEEK_RESPONSE;
        this.streamOffset = streamOffset;
    }

    /**
     * Serialize the message into the given bytebuffer.
     *
     * @param buffer The buffer to serialize to.
     */
    @Override
    public void serialize(ByteBuf buffer) {
        super.serialize(buffer);
        buffer.writeByte(result.asByte());
        buffer.writeLong(streamOffset);
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
        result = readResultTypeMap.get(buffer.readByte());
        streamOffset = buffer.readLong();
    }
}
