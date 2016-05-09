package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import javafx.util.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** A view of the address space implemented by Corfu.
 *
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class AddressSpaceView extends AbstractView {

    /** A cache for read results. */
    static LoadingCache<Long, ILogUnitEntry> readCache;

    /** A cache for mapping (streamID, offset) -> address, to be looked up in readCache. */
    static Cache<Pair<UUID, Long>, Long> streamMapCache;

    /** A cache for stream addresses. */
    static LoadingCache<UUID, Set<Long>> streamAddressCache;

    /** Duration before retrying an empty read. */
    @Getter
    @Setter
    Duration emptyDuration = Duration.ofMillis(5000L);

    public AddressSpaceView(CorfuRuntime runtime)
    {
        super(runtime);
        // We don't lock readCache, this should be ok in the rare
        // case we generate a second readCache as it won't be pointed to.
        if (readCache == null) {
            resetCaches();
        }
        else {
            log.debug("Read cache already built, re-using existing read cache.");
        }
    }

    /** Reset all in-memory caches. */
    public void resetCaches()
    {
        readCache = Caffeine.<Long, ILogUnitEntry>newBuilder()
                .<Long, ILogUnitEntry>weigher((k,v) -> v.getSizeEstimate())
                .maximumWeight(runtime.getMaxCacheSize())
                .build(new CacheLoader<Long, ILogUnitEntry>() {
                    @Override
                    public ILogUnitEntry load(Long aLong) throws Exception {
                        return cacheFetch(aLong);
                    }

                    @Override
                    public Map<Long, ILogUnitEntry> loadAll(Iterable<? extends Long> keys) throws Exception {
                        return cacheFetch((Iterable<Long>) keys);
                    }
                });

        streamAddressCache = Caffeine.newBuilder()
                .build(this::getStream);

        streamMapCache = Caffeine.newBuilder().build();
    }

    /** Learn about a stream for the first time.
     *  This method will dump all learned stream entries into the stream.
     *
     * @param streamID  The ID of a stream.
     * @return          The long
     */
    private Set<Long> getStream(UUID streamID)
    {
        return layoutHelper(l -> {
            Set<Long> rSet = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
                    for (Layout.LayoutSegment s : l.getSegments()) {
                        AbstractReplicationView v = AbstractReplicationView
                                .getReplicationView(l, s.getReplicationMode(), s);
                        Map<Long, ILogUnitEntry> r = v.read(streamID);
                        if (!runtime.cacheDisabled) {
                            readCache.putAll(r);
                        }
                        rSet.addAll(r.keySet());
                    }
            return rSet;
            });
    }

    /** Learn about a stream for the first time, bypassing the cache.
     *  This method will dump all learned stream entries into the stream.
     *
     * @param streamID  The ID of a stream.
     * @return          The long
     */
    private Map<Long, ILogUnitEntry> fetchStream(UUID streamID)
    {
        return layoutHelper(l -> {
            Map<Long, ILogUnitEntry> rMap = new ConcurrentHashMap<>();
            for (Layout.LayoutSegment s : l.getSegments()) {
                AbstractReplicationView v = AbstractReplicationView
                        .getReplicationView(l, s.getReplicationMode(), s);
                Map<Long, ILogUnitEntry> r = v.read(streamID);
                if (!runtime.cacheDisabled) {
                    readCache.putAll(r);
                }
                rMap.putAll(r);
            }
            return rMap;
        });
    }

    /**
     * Write the given object to an address and streams.
     *
     * @param address           An address to write to.
     * @param stream        The streams which will belong on this entry.
     * @param data          The data to write.
     * @param backpointerMap
     */
    public void write(long address, Set<UUID> stream, Object data, Map<UUID, Long> backpointerMap)
    throws OverwriteException
    {
        int numBytes = layoutHelper(l -> AbstractReplicationView.getReplicationView(l, l.getReplicationMode(address),
                l.getSegment(address))
                   .write(address, stream, data, backpointerMap));

        // Must generate a cached entry as it is used by some entry types before write.
        AbstractReplicationView.CachedLogUnitEntry cachedEntry =
                new AbstractReplicationView.CachedLogUnitEntry(LogUnitReadResponseMsg.ReadResultType.DATA,
                        data, address, runtime, numBytes);
        cachedEntry.setBackpointerMap(backpointerMap);
        cachedEntry.setStreams(stream);
        if (data instanceof LogEntry)
        {
            ((LogEntry) data).setEntry(cachedEntry);
            ((LogEntry) data).setRuntime(runtime);
        }

        // Insert this write to our local cache.
        if (!runtime.isCacheDisabled()) {
            readCache.put(address, cachedEntry);
        }
    }

    /**
     * Read the given object from an address and streams.
     *
     * @param address An address to read from.
     * @return        A result, which be cached.
     */
    public ILogUnitEntry read(long address)
    {
        if (!runtime.isCacheDisabled()) {
            return readCache.get(address);
        }
        return fetch(address);
    }

    /**
     * Read the given object from the streamID and offset.
     *
     * @param streamID  Stream to read from.
     * @param offset    Offset within the stream to read from.
     * @return          A result, which be cached.
     */
    public ILogUnitEntry read(UUID streamID, long offset)
    {
        ILogUnitEntry ret;
        if (!runtime.isCacheDisabled()) {
            Long address = streamMapCache.getIfPresent(new Pair(streamID, offset));
            if (address != null) {
                ret = readCache.getIfPresent(address);
                if (ret != null)
                    return ret;
            }
        }
        return streamFetch(streamID, offset);
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param addresses An address range to read from.
     * @return        A result, which be cached.
     */
    public Map<Long, ILogUnitEntry> read(RangeSet<Long> addresses)
    {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(Utils.discretizeRangeSet(addresses));
        }
        return this.cacheFetch(Utils.discretizeRangeSet(addresses));
    }

    /**
     * Read the given object from a range of addresses.
     *
     * @param  stream An address range to read from.
     * @return        A result, which be cached.
     */
    public Map<Long, ILogUnitEntry> readPrefix(UUID stream)
    {

        if (!runtime.isCacheDisabled()) {
            return readCache.getAll(streamAddressCache.get(stream));
        }
        return fetchStream(stream);
    }


    /**
     * Fetch an address for insertion into the cache.
     * @param address An address to read from.
     * @return        A result to be cached. If the readresult is empty,
     *                This entry will be scheduled to self invalidate.
     */
    private ILogUnitEntry cacheFetch(long address)
    {
        log.trace("Cache miss @ {}, fetching.", address);
        ILogUnitEntry result = fetch(address);
        if (result.getResultType() == LogUnitReadResponseMsg.ReadResultType.EMPTY)
        {
            //schedule an eviction
            CompletableFuture.runAsync(() -> {
                log.trace("Evicting empty entry at {}.", address);
                CFUtils.runAfter(emptyDuration, () -> {
                    readCache.invalidate(address);
                });
            });
        }
        return result;
    }

    /**
     * Fetch an address for insertion into the cache.
     * @param addresses An address to read from.
     * @return        A result to be cached. If the readresult is empty,
     *                This entry will be scheduled to self invalidate.
     */
    private Map<Long, ILogUnitEntry> cacheFetch(Iterable<Long> addresses)
    {
        // for each address, figure out which replication group it goes to.
        Map<AbstractReplicationView, RangeSet<Long>> groupMap = new ConcurrentHashMap<>();
        return layoutHelper(l -> {
                    for (Long a : addresses) {
                        AbstractReplicationView v = AbstractReplicationView
                                .getReplicationView(l, l.getReplicationMode(a), l.getSegment(a));
                        groupMap.computeIfAbsent(v, x -> TreeRangeSet.<Long>create())
                                .add(Range.singleton(a));
                    }
                    Map<Long, ILogUnitEntry> result =
                            new ConcurrentHashMap<Long, ILogUnitEntry>();
                    for (AbstractReplicationView vk : groupMap.keySet())
                    {
                        result.putAll(vk.read(groupMap.get(vk)));
                    }
                    return result;
                }
        );
    }


    /**
     * Explicitly fetch a given address, bypassing the cache.
     * @param address An address to read from.
     * @return        A result, which will be uncached.
     */
    public ILogUnitEntry fetch(long address)
    {
        return layoutHelper(l -> AbstractReplicationView
                        .getReplicationView(l, l.getReplicationMode(address), l.getSegment(address))
                        .read(address)
        ).setRuntime(runtime);
    }

    public ILogUnitEntry streamFetch(UUID streamID, long offset)
    {
        //TODO: for now, only returns the first segment, because you cannot mix replication modes..
        //TODO: reconfigs that are correct for ReplexReplicationMode
        return layoutHelper(l -> AbstractReplicationView
                .getReplicationView(l, Layout.ReplicationMode.REPLEX_REPLICATION,
                        l.getSegments().get(l.getSegments().size() - 1))
                .streamRead(streamID, offset)
        ).setRuntime(runtime);
    }

    /**
     * Return the entry in the given stream that has a globalOffset as close to or greater than globalOffset.
     *
     * @param globalOffset  The global address for reference.
     * @param streamID      The stream from which to fetch the desired entry.
     * @return              The log entry.
     */
    public ILogUnitEntry seek(long globalOffset, UUID streamID, long maxLocalOffset) {
        //TODO: for now, only implemented in Replex logging units.
        if (!runtime.replexEnabled)
            throw new UnsupportedOperationException("Seeks are not currently supported");

        return layoutHelper(l -> AbstractReplicationView
                        .getReplicationView(l, Layout.ReplicationMode.REPLEX_REPLICATION,
                                l.getSegments().get(l.getSegments().size() - 1))
                        .seek(globalOffset, streamID, maxLocalOffset)
        ).setRuntime(runtime);
    }

    /**
     * Fill a hole at the given address.
     * @param address An address to hole fill at.
     */
    public void fillHole(long address)
    throws OverwriteException
    {
        layoutHelper(
                l -> {AbstractReplicationView
                .getReplicationView(l, l.getReplicationMode(address), l.getSegment(address))
                .fillHole(address);
                return null;}
        );
    }

    public void compactAll() {
        layoutHelper( l-> {
        for (Layout.LayoutSegment s : l.getSegments()) {
            for (Layout.LayoutStripe ls: s.getStripes())
            {
                for (String server : ls.getLogServers()) {
                    l.getRuntime().getRouter(server).getClient(LogUnitClient.class).forceCompact();
                }
            }
        }
        return null;
        });
    }
}
