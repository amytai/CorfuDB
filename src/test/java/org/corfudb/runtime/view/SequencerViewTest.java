package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/23/15.
 */
public class SequencerViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canAcquireFirstToken() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(0);
    }

    @Test
    public void tokensAreIncrementing() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(1);
    }

    @Test
    public void checkTokenWorks() {
        CorfuRuntime r = getDefaultRuntime();
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 1).getToken())
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.emptySet(), 0).getToken())
                .isEqualTo(0);
    }

    @Test
    public void checkStreamTokensWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getToken())
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(0);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getToken())
                .isEqualTo(1);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getToken())
                .isEqualTo(1);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(0);
    }

    @Test
    public void checkBackPointersWork() {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("stream B".getBytes());

        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, -1L);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, -1L);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 0).getBackpointerMap())
                .isEmpty();
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(r.getSequencerView().nextToken(Collections.singleton(streamB), 1).getBackpointerMap())
                .containsEntry(streamB, 1L);
    }

    @Test
    public void replexAddressFlagWorks() throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = UUID.nameUUIDFromBytes("streamA".getBytes());
        UUID streamB = UUID.nameUUIDFromBytes("streamB".getBytes());
        HashSet<UUID> streams = new HashSet();
        streams.add(streamA);
        streams.add(streamB);

        assertThat(
                r.getSequencerView().nextTokenWrapper(Collections.singleton(streamA), 1, true).getBackpointerMap())
                .containsEntry(streamA, 0L);
        assertThat(
                r.getSequencerView().nextToken(Collections.singleton(streamA), 0).getToken())
                .isEqualTo(0L);
        assertThat(
                r.getSequencerView().nextTokenWrapper(Collections.singleton(streamA), 1, true).getBackpointerMap())
                .containsEntry(streamA, 1L);
        assertThat(
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1).getBackpointerMap())
                .containsEntry(streamA, 1L);

        Map<UUID, Long> map = r.getSequencerView().nextToken(streams, 1).getBackpointerMap();
        assertThat(map).containsEntry(streamA, 2L);
        assertThat(map).containsEntry(streamB, -1L);

        map = r.getSequencerView().nextTokenWrapper(streams, 1, true).getBackpointerMap();
        assertThat(map).containsEntry(streamA, 4L);
        assertThat(map).containsEntry(streamB, 1L);
    }
}
