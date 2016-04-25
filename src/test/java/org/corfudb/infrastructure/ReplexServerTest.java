package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import javafx.util.Pair;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Collections;

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
        ReplexLogUnitWriteMsg m = new ReplexLogUnitWriteMsg(CorfuRuntime.getStreamID("a"), 0L);
        m.setRank(0L);
        m.setPayload("0".getBytes());
        sendMessage(m);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .doesNotContainKey(IMetadata.LogUnitMetadataType.REPLEX_COMMIT);

        ReplexCommitMsg cm = new ReplexCommitMsg(CorfuRuntime.getStreamID("a"), 0L);
        cm.setReplexCommit(true);
        sendMessage(cm);

        assertThat(rs.dataCache.getIfPresent(new Pair(CorfuRuntime.getStreamID("a"), 0L)).getMetadataMap())
                .containsEntry(IMetadata.LogUnitMetadataType.REPLEX_COMMIT, true);

        rs.shutdown();
    }
}

