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
public class ReplexLogUnitReadResponseMsg extends LogUnitReadResponseMsg {

    public long globalAddress; // The global address of the returned entry.

    public ReplexLogUnitReadResponseMsg(ReadResultType result, long globalAddress)
    {
        super(result);
        this.msgType = CorfuMsgType.REPLEX_READ_RESPONSE;
        this.globalAddress = globalAddress;
    }

    public ReplexLogUnitReadResponseMsg(LogUnitEntry entry, long globalAddress)
    {
        super(entry);
        this.msgType = CorfuMsgType.REPLEX_READ_RESPONSE;
        this.globalAddress = globalAddress;
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
        buffer.writeLong(globalAddress);
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
        globalAddress = buffer.readLong();
    }
}
