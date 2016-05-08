package org.corfudb.runtime.clients;

import com.google.common.collect.*;
import javafx.util.Pair;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ReplexServer;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogUnitReadRequestMsg;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by amytai on 4/25/16.
 */
public class ReplexLogUnitClientTest extends AbstractClientTest {

    ReplexLogUnitClient client;

    @Override
    Set<IServer> getServersForTest() {
        return new ImmutableSet.Builder<IServer>()
                .add(new ReplexServer(defaultOptionsMap()))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new ReplexLogUnitClient();
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseClient())
                .add(client)
                .build();
    }

    @Test
    public void canReadWrite()
    throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 0L), 0L, 0, testString).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString);
    }

    @Test
    public void canReadCommitBit() throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 0L), 0L, 0, testString).get();

        LogUnitReadResponseMsg.ReadResult r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getMetadataMap()).doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);

        client.writeCommit(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 0L), true);

        r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getMetadataMap()).containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);
    }

    @Test
    public void canReadWriteStreamMap() throws Exception {
        byte[] testString = "hello world".getBytes();
        HashMap<UUID, Long> streams = new HashMap();
        streams.put(CorfuRuntime.getStreamID("a"), 0L);
        streams.put(CorfuRuntime.getStreamID("b"), 0L);
        client.write(streams, 0L, 0, testString).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString);

        r = client.read(CorfuRuntime.getStreamID("b"), 0L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString);
    }

    @Test
    public void seekCorrectly() throws Exception {
        byte[] testString0 = "hello world0".getBytes();
        byte[] testString1 = "hello world1".getBytes();
        byte[] testString2 = "hello world2".getBytes();
        byte[] testString3 = "hello world3".getBytes();
        HashMap<UUID, Long> streams = new HashMap();
        streams.put(CorfuRuntime.getStreamID("a"), 0L);
        streams.put(CorfuRuntime.getStreamID("b"), 0L);

        client.write(streams, 0L, 0, testString0).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 1L), 1L, 0, testString1).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("b"), 1L), 2L, 0, testString2).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 2L), 3L, 0, testString3).get();

        LogUnitReadResponseMsg.ReplexSeekResult r = client.seek(1L, CorfuRuntime.getStreamID("a"), 2L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString1);
        assertThat(r.getLocalOffset()).isEqualTo(1L);

        LogUnitReadResponseMsg.ReadResult rr = client.read(CorfuRuntime.getStreamID("a"), 2L).get();
        assertThat(rr.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(rr.getPayload())
                .isEqualTo(testString3);
        assertThat(rr.getAddress()).isEqualTo(3L);

        r = client.seek(1L, CorfuRuntime.getStreamID("b"), 1L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString2);
        assertThat(r.getLocalOffset()).isEqualTo(1L);
        assertThat(r.getAddress()).isEqualTo(2L);
    }

    @Test
    public void CanReadRanges() throws Exception {
        byte[] testString0 = "hello world0".getBytes();
        byte[] testString1 = "hello world1".getBytes();
        byte[] testString2 = "hello world2".getBytes();
        byte[] testString3 = "hello world3".getBytes();
        HashMap<UUID, Long> streams = new HashMap();
        streams.put(CorfuRuntime.getStreamID("a"), 0L);
        streams.put(CorfuRuntime.getStreamID("b"), 0L);

        client.write(streams, 0L, 0, testString0).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 1L), 1L, 0, testString1).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("b"), 1L), 2L, 0, testString2).get();
        client.write(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 2L), 3L, 0, testString3).get();

        Map<UUID, RangeSet<Long>> rangeMap = new HashMap<>();
        RangeSet<Long> aSet = TreeRangeSet.create();
        aSet.add(Range.closed(0L, 2L));
        RangeSet<Long> bSet = TreeRangeSet.create();
        bSet.add(Range.closed(0L, 2L));

        rangeMap.put(CorfuRuntime.getStreamID("a"), aSet);
        rangeMap.put(CorfuRuntime.getStreamID("b"), bSet);

        Map<Pair<UUID, Long>, LogUnitReadResponseMsg.ReadResult> resultMap = client.readRange(rangeMap).get();

        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getAddress()).isEqualTo(0L);
        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getPayload()).isEqualTo(testString0);

        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 1L)).getAddress()).isEqualTo(1L);
        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 1L)).getPayload()).isEqualTo(testString1);

        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 2L)).getAddress()).isEqualTo(3L);
        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("a"), 2L)).getPayload()).isEqualTo(testString3);

        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("b"), 0L)).getAddress()).isEqualTo(0L);
        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("b"), 0L)).getPayload()).isEqualTo(testString0);

        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("b"), 1L)).getAddress()).isEqualTo(2L);
        assertThat(resultMap.get(new Pair(CorfuRuntime.getStreamID("b"), 1L)).getPayload()).isEqualTo(testString2);

        assertThat(resultMap).doesNotContainKey(new Pair(CorfuRuntime.getStreamID("b"), 2L));
    }

}