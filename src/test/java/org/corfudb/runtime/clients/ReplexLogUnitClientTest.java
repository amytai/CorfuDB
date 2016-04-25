package org.corfudb.runtime.clients;

import com.google.common.collect.*;
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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
        client.write(CorfuRuntime.getStreamID("a"), 0L, 0, testString).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString);
    }

    @Test
    public void canReadCommitBit() throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(CorfuRuntime.getStreamID("a"), 0L, 0, testString).get();

        LogUnitReadResponseMsg.ReadResult r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getMetadataMap()).doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);

        client.writeCommit(CorfuRuntime.getStreamID("a"), 0L, true);

        r = client.read(CorfuRuntime.getStreamID("a"), 0L).get();
        assertThat(r.getMetadataMap()).containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);
    }

}
