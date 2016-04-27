package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import javafx.util.Pair;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by amytai on 4/24/16.
 */
public class ReplexServerTest extends AbstractServerTest {

    @Override
    public IServer getDefaultServer() {
        return new ReplexServer(new ImmutableMap.Builder<String,Object>()
                .put("--memory", true)
                .put("--single", false)
                .put("--sync", true)
                .put("--max-cache", 1000000)
                .build());
    }

    @Test
    public void checkCommitBitsAreSet()
            throws Exception
    {
        ReplexServer rs = (ReplexServer) getDefaultServer();

        this.router.setServerUnderTest(rs);
//        // First write a message
        ReplexLogUnitWriteMsg m = new ReplexLogUnitWriteMsg(
                Collections.singletonMap(CorfuRuntime.getStreamID("a"), 0L));
        m.setRank(0L);
        m.setPayload("0".getBytes());
        sendMessage(m);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);

        ReplexCommitMsg cm = new ReplexCommitMsg(Collections.singletonMap(CorfuRuntime.getStreamID("a"), 0L));
        cm.setReplexCommit(true);
        sendMessage(cm);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);

        rs.shutdown();
    }

    @Test
    public void checkMapOfStreamsWorks() throws Exception {
        ReplexServer rs = (ReplexServer) getDefaultServer();

        this.router.setServerUnderTest(rs);
//        // First write a message
        HashMap<UUID, Long> streams = new HashMap();
        streams.put(CorfuRuntime.getStreamID("a"), 0L);
        streams.put(CorfuRuntime.getStreamID("b"), 0L);
        ReplexLogUnitWriteMsg m = new ReplexLogUnitWriteMsg(streams);
        m.setRank(0L);
        m.setPayload("0".getBytes());
        sendMessage(m);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);
        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("b"), 0L)).getMetadataMap())
                .doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);

        ReplexCommitMsg cm = new ReplexCommitMsg(streams);
        cm.setReplexCommit(true);
        sendMessage(cm);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);
        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("b"), 0L)).getMetadataMap())
                .containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);

        rs.shutdown();
    }
}

