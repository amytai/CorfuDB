package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ReplexServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by amytai on 5/9/16.
 */
public class ReplexStreamViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStream()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));

        ReplexServer rs = new ReplexServer(defaultOptionsMap());

        addServerForTest(getEndpoint(9001), rs);
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();

        Layout.LayoutSegment seg = new Layout.LayoutSegment(
                Layout.ReplicationMode.REPLEX_REPLICATION,
                0L,
                -1L,
                ImmutableList.<Layout.LayoutStripe>builder()
                        .add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9000))))
                        .build()
        );
        List<Layout.LayoutStripe> replexStripes = new ArrayList<>();
        replexStripes.add(new Layout.LayoutStripe(Collections.singletonList(getEndpoint(9001))));
        seg.setReplexStripes(replexStripes);

        setLayout(new Layout(
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(getEndpoint(9000)),
                Collections.singletonList(seg),
                1L
        ));


        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        ReplexStreamView sv = new ReplexStreamView(r, streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

   /* @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamConcurrent()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(100, i -> sv.write(testPayload));
        executeScheduled(8, 10, TimeUnit.SECONDS);

        scheduleConcurrently(100, i-> assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes()));
        executeScheduled(8, 10, TimeUnit.SECONDS);
        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime()
                .setBackpointersDisabled(true)
                .connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(100, i -> sv.write(testPayload));
        executeScheduled(8, 10, TimeUnit.SECONDS);

        scheduleConcurrently(100, i-> assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes()));
        executeScheduled(8, 10, TimeUnit.SECONDS);
        assertThat(sv.read())
                .isEqualTo(null);
    }



    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromCachedStream()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect()
                .setCacheDisabled(false);
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

        @Test
    @SuppressWarnings("unchecked")
    public void streamCanSurviveOverwriteException()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // write without reserving a token
        r.getAddressSpaceView().fillHole(0);

        // Write to the stream, and read back. The hole should be filled.
        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamWillHoleFill()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Generate a hole.
        r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // Write to the stream, and read back. The hole should be filled.
        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload())
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }



    @Test
    @SuppressWarnings("unchecked")
    public void streamWithHoleFill()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();
        byte[] testPayload2 = "hello world2".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        //generate a stream hole
        SequencerClient.TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        r.getAddressSpaceView().fillHole(tr.getToken());

        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        r.getAddressSpaceView().fillHole(tr.getToken());

        sv.write(testPayload2);

        //make sure we can still read the stream.
        assertThat(sv.read().getPayload())
                .isEqualTo(testPayload);

        assertThat(sv.read().getPayload())
                .isEqualTo(testPayload2);
    }*/
}
