package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.*;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import javafx.util.Pair;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResultType;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.LogUnitEntry;

/**
 * Created by amytai on 4/13/15.
 *
 * A server responsible for providing Replex storage for the Corfu Distributed Shared Log.
 *
 * Replexes should be a generalized logging unit, but for now, they provide the (streamID, offset) ->  LogUnitEntry
 * interface.
 *
 * FOR NOW, REPLEX SERVERS ARE IN-MEMORY, AND NOT PERSISTENT. So, most of the FileHandle-ing code is commented out.
 * Similarly, no trimming, garbage collection, stream compaction, or contiguous tail is supported on replexes.
 *
 * All reads and writes go through a cache.
 */
@Slf4j
public class ReplexServer implements IServer {

    /** The options map. */
    Map<String,Object> opts;

    /** The log file prefix, which can be null if the server is in memory. */
    String prefix;

    @Data
    class FileHandle {
        final AtomicLong filePointer;
        final FileChannel channel;
        final FileLock lock;
        final Set<Long> knownAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
        @Getter(lazy=true)
        private final MappedByteBuffer byteBuffer = getMappedBuffer();
        public ByteBuffer getMapForRegion(int offset, int size)
        {
            ByteBuffer o = getByteBuffer().duplicate();
            o.position(offset);
            return o.slice();
        }
        private MappedByteBuffer getMappedBuffer() {
            try {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0L, Integer.MAX_VALUE);
            }
            catch (IOException ie)
            {
                log.error("Failed to map buffer for channel.");
                throw new RuntimeException(ie);
            }
        }
    }

    /** A map mapping to file channels. */
    Map<Long, FileHandle> channelMap;

    /** The garbage collection thread. */
    Thread gcThread;

    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    @Getter
    long contiguousTail;

    /**
     * The addresses that this unit has seen, temporarily until they are integrated into the contiguousTail.
     */
    Set<Address> seenAddressesTemp;

    @Data
    class Address implements Comparable<Address> {
        final long logAddress;
        final Set<UUID> StreamIDs;

        @Override
        public int compareTo(Address o) {
            return Long.compare(logAddress, o.logAddress);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(logAddress);
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof Address && logAddress == ((Address)obj).logAddress;
        }
    }

    /**
     * A range set representing trimmed addresses on the log unit.
     */
    RangeSet<Long> trimRange;

    ConcurrentHashMap<UUID, Long> trimMap;

    IntervalAndSentinelRetry gcRetry;

    AtomicBoolean running = new AtomicBoolean(true);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    Cache<Pair<UUID, Long>, LogUnitEntry> dataCache;

    long maxCacheSize;

    /** This cache services requests for stream addresses.
     */
    //LoadingCache<UUID, RangeSet<Long>> streamCache;

    /**
     * A scheduler, which is used to schedule periodic tasks like garbage collection.
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("LogUnit-Maintenance-%d")
                            .build());

    public ReplexServer(Map<String, Object> opts)
    {
        this.opts = opts;

        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));

        //if ((Boolean)opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
        /*    reset();
        }
        else {
            channelMap = new ConcurrentHashMap<>();
            prefix = opts.get("--log-path") + File.separator + "log";
        }*/

        reset();

//        scheduler.scheduleAtFixedRate(this::compactTail,
//                Utils.getOption(opts, "--compact", Long.class, 60L),
//                Utils.getOption(opts, "--compact", Long.class, 60L),
//                TimeUnit.SECONDS);
//
//        gcThread = new Thread(this::runGC);
//        gcThread.start();
    }

    /*
    @Data
    static class LogFileHeader {
        static final String magic = "CORFULOG";
        final int version;
        final long flags;
        ByteBuffer getBuffer()
        {
            ByteBuffer b = ByteBuffer.allocate(64);
            // 0: "CORFULOG" header(8)
            b.put(magic.getBytes(Charset.forName("UTF-8")),0, 8);
            // 8: Version number(4)
            b.putInt(version);
            // 12: Flags (8)
            b.putLong(flags);
            // 20: Reserved (54)
            b.position(64);
            b.flip();
            return b;
        }
        static LogFileHeader fromBuffer(ByteBuffer buffer)
        {
            byte[] bMagic = new byte[8];
            buffer.get(bMagic, 0, 8);
            if (!new String(bMagic).equals(magic))
            {
                log.warn("Encountered invalid magic, expected {}, got {}", magic, new String(bMagic));
                throw new RuntimeException("Invalid header magic!");
            }
            return new LogFileHeader(buffer.getInt(), buffer.getLong());
        }
    }
    */

//    public synchronized void compactTail() {
//        long numEntries = 0;
//        List<Address> setCopy = new ArrayList<>(seenAddressesTemp);
//        Collections.sort(setCopy);
//        for (Address i : setCopy)
//        {
//            if (i.getLogAddress() == contiguousTail + 1)
//            {
//                contiguousTail = i.getLogAddress();
//                seenAddressesTemp.remove(i);
//                numEntries++;
//
//                if (i.getStreamIDs().size() > 0)
//                {
//                    for (UUID stream : i.getStreamIDs())
//                    {
//                        RangeSet<Long> currentSet = streamCache.get(stream);
//                        currentSet.add(Range.singleton(i.getLogAddress()));
//                        streamCache.put(stream, currentSet);
//                    }
//                }
//            }
//            else {
//                break;
//            }
//        }
//        if (numEntries > 0) {
//            log.debug("Completed tail compaction, compacted {} entries, tail is now at {}", numEntries, contiguousTail);
//        }
//    }

//    /** Write the header for a Corfu log file.
//     *
//     * @param fc            The filechannel to use.
//     * @param pointer       The pointer to increment to the start position.
//     * @param version       The version number to write to the header.
//     * @param flags         Flags, if any to write to the header.
//     * @throws IOException
//     */
//    public void writeHeader(FileChannel fc, AtomicLong pointer, int version, long flags)
//            throws IOException
//    {
//        LogFileHeader lfg = new LogFileHeader(version, flags);
//        ByteBuffer b = lfg.getBuffer();
//        pointer.getAndAdd(b.remaining());
//        fc.write(b);
//        fc.force(true);
//    }
//
//    /** Read the header for a Corfu log file.
//     *
//     * @param fc            The filechannel to use.
//     * @throws IOException
//     */
//    public LogFileHeader readHeader(FileChannel fc)
//            throws IOException
//    {
//        ByteBuffer b = fc.map(FileChannel.MapMode.READ_ONLY, 0, 64);
//        return LogFileHeader.fromBuffer(b);
//    }
//
//    /** Write a log entry to a file.
//     *
//     * @param fh            The file handle to use.
//     * @param address       The address of the entry.
//     * @param entry         The LogUnitEntry to write.
//     */
//    public void writeEntry(FileHandle fh, long address, LogUnitEntry entry)
//        throws IOException
//    {
//        ByteBuf metadataBuffer = Unpooled.buffer();
//        LogUnitMetadataMsg.bufferFromMap(metadataBuffer, entry.getMetadataMap());
//        int entrySize = entry.getBuffer().writerIndex() + metadataBuffer.writerIndex() + 24;
//        long pos = fh.getFilePointer().getAndAdd(entrySize);
//        ByteBuffer o = fh.getMapForRegion((int)pos, entrySize);
//        o.putInt(0x4C450000); // Flags
//        o.putLong(address); // the log unit address
//        o.putInt(entrySize); // Size
//        o.putInt(metadataBuffer.writerIndex()); // the metadata size
//        o.put(metadataBuffer.nioBuffer());
//        o.put(entry.buffer.nioBuffer());
//        metadataBuffer.release();
//        o.putShort(2, (short) 1); // written flag
//        o.flip();
//    }
//
//    /** Find a log entry in a file.
//     * @param fh            The file handle to use.
//     * @param address       The address of the entry.
//     * @return              The log unit entry at that address, or NULL if there was no entry.
//     */
//    public LogUnitEntry readEntry(FileHandle fh, long address)
//        throws IOException
//    {
//        ByteBuffer o = fh.getMapForRegion(64, (int)fh.getChannel().size());
//        while (o.hasRemaining())
//        {
//            short magic = o.getShort();
//            if (magic != 0x4C45)
//            {
//                return null;
//            }
//            short flags = o.getShort();
//            long addr = o.getLong();
//            if (address == -1) {
//            fh.knownAddresses.add(addr); }
//            int size = o.getInt();
//            if (addr != address)
//            {
//                o.position(o.position() + size-16); //skip over (size-20 is what we haven't read).
//                log.trace("Read address {}, not match {}, skipping. (remain={})", addr, address, o.remaining());
//            }
//            else {
//                log.debug("Entry at {} hit, reading (size={}).", address, size);
//                if (flags % 2 == 0) {
//                    log.error("Read a log entry but the write was torn, aborting!");
//                    throw new IOException("Torn write detected!");
//                }
//                int metadataMapSize = o.getInt();
//                ByteBuf mBuf = Unpooled.wrappedBuffer(o.slice());
//                o.position(o.position() + metadataMapSize);
//                ByteBuffer dBuf = o.slice();
//                dBuf.limit(size - metadataMapSize - 24);
//                return new LogUnitEntry(Unpooled.wrappedBuffer(dBuf),
//                        LogUnitMetadataMsg.mapFromBuffer(mBuf),
//                        false,
//                        true);
//            }
//        }
//        return null;
//    }
//
//    /** Gets the file channel for a particular address, creating it
//     * if is not present in the map.
//     * @param address   The address to open.
//     * @return          The FileChannel for that address.
//     */
//    public FileHandle getChannelForAddress(long address)
//    {
//        return channelMap.computeIfAbsent(address/10000, a -> {
//            String filePath = prefix + a.toString();
//            try {
//                FileChannel fc = FileChannel.open(FileSystems.getDefault().getPath(filePath),
//                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
//                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
//
//                FileLock fl = fc.lock();
//
//                AtomicLong fp = new AtomicLong();
//                writeHeader(fc, fp, 1, 0);
//                log.info("Opened new log file at {}", filePath);
//                FileHandle fh = new FileHandle(fp, fc, fl);
//                // The first time we open a file we should read to the end, to load the
//                // map of entries we already have.
//                readEntry(fh, -1);
//                return fh;
//            }
//            catch (IOException e)
//            {
//                log.error("Error opening file {}", a, e);
//                throw new RuntimeException(e);
//            }
//        });
//    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch(msg.getMsgType()) {
            case REPLEX_WRITE:
                ReplexLogUnitWriteMsg writeMsg = (ReplexLogUnitWriteMsg) msg;
                log.trace("Handling write request for {} addresses)", writeMsg.getStreamPairs().size());
                write(writeMsg, ctx, r);
                break;
            case REPLEX_READ_REQUEST:
                ReplexLogUnitReadRequestMsg readMsg = (ReplexLogUnitReadRequestMsg) msg;
                log.trace("Handling read request for address ({}, {})", readMsg.getStreamID(), readMsg.getOffset());
                read(readMsg, ctx, r);
                break;
//            case READ_RANGE:
//                CorfuRangeMsg rangeReadMsg = (CorfuRangeMsg) msg;
//                log.trace("Handling read request for address ranges {}", rangeReadMsg.getRanges());
//                read(rangeReadMsg, ctx, r);
//                break;
//            case GC_INTERVAL:
//            {
//                LogUnitGCIntervalMsg m = (LogUnitGCIntervalMsg) msg;
//                log.info("Garbage collection interval set to {}", m.getInterval());
//                gcRetry.setRetryInterval(m.getInterval());
//            }
//            break;
//            case FORCE_GC:
//            {
//                log.info("GC forced by client {}", msg.getClientID());
//                gcThread.interrupt();
//            }
//            break;
            case REPLEX_FILL_HOLE: {
                ReplexLogUnitFillHoleMsg m = (ReplexLogUnitFillHoleMsg) msg;
                log.debug("Hole fill requested at ({}, {})", m.getStreamID(), m.getOffset());
                //TODO: use get or getIsPresent?
                dataCache.get(new Pair(m.getStreamID(), m.getOffset()), (address) -> new LogUnitEntry());
                r.sendResponse(ctx, m, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            }
            break;
            case REPLEX_COMMIT:
            {
                ReplexCommitMsg m = (ReplexCommitMsg) msg;
                log.debug("Setting commit bit at {} local stream addresses to {}", m.getStreamPairs().size(),
                        m.getMetadataMap().get(IMetadata.LogUnitMetadataType.REPLEX_COMMIT));
                for (UUID streamID : m.getStreamPairs().keySet()) {
                    Pair key = new Pair(streamID, m.getStreamPairs().get(streamID));
                    LogUnitEntry e = dataCache.getIfPresent(key);
                    if (e == null)
                        continue;
                    e.setReplexCommit(m.getReplexCommit());
                }
                r.sendResponse(ctx, m, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));

            }
//            case TRIM:
//            {
//                LogUnitTrimMsg m = (LogUnitTrimMsg) msg;
//                trimMap.compute(m.getStreamID(), (key, prev) ->
//                        prev == null ? m.getPrefix() : Math.max(prev, m.getPrefix()));
//                log.debug("Trim requested at prefix={}", m.getPrefix());
//            }
//            break;
//            case FORCE_COMPACT:
//            {
//                log.info("Compaction forced by client {}", msg.getClientID());
//                compactTail();
//            }
//            break;
//            case GET_CONTIGUOUS_TAIL: {
//                CorfuUUIDMsg m = (CorfuUUIDMsg) msg;
//                if (m.getId() == null) {
//                    r.sendResponse(ctx, m, new LogUnitTailMsg(contiguousTail));
//                } else {
//                    r.sendResponse(ctx, m, new LogUnitTailMsg(contiguousTail, streamCache.get(m.getId())));
//                }
//            }
//            break;
//            case STREAM_READ: {
//                CorfuUUIDMsg m = (CorfuUUIDMsg) msg;
//                if (m.getId() == null) {
//                    r.sendResponse(ctx, m, new CorfuMsg(CorfuMsg.CorfuMsgType.NACK));
//                } else {
//                    CorfuRangeMsg rm = new CorfuRangeMsg(streamCache.get(m.getId()));
//                    rm.copyBaseFields(m);
//                    read(rm, ctx, r);
//                }
//            }
//            break;
        }
    }

    @Override
    public void reset() {
        contiguousHead = 0L;
        contiguousTail = -1L;
        trimRange = TreeRangeSet.create();
        seenAddressesTemp = Collections.newSetFromMap(new ConcurrentHashMap<>());

        if (dataCache != null)
        {
            /** Free all references */
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        }

        dataCache = Caffeine.newBuilder()
                .<Pair<UUID, Long>, LogUnitEntry>weigher((k, v) -> v.buffer == null ? 1 : v.buffer.readableBytes())
                .maximumWeight(maxCacheSize).build();
//                .removalListener(this::handleEviction)
//                .writer(new CacheWriter<Long, LogUnitEntry>() {
//                    @Override
//                    public void write(Long address, LogUnitEntry entry) {
//                        if (dataCache.getIfPresent(address) != null) {// || seenAddresses.contains(address)) {
//                            throw new RuntimeException("overwrite");
//                        }
//                        seenAddressesTemp.add(new Address(address, entry.getStreams()));
//                        if (!entry.isPersisted && prefix != null) { //don't persist an entry twice.
//                            //evict the data by getting the next pointer.
//                            try {
//                                // make sure the entry doesn't currently exist...
//                                // (probably need a faster way to do this - high watermark?)
//                                FileHandle fh = getChannelForAddress(address);
//                                if (!fh.getKnownAddresses().contains(address)) {
//                                    fh.getKnownAddresses().add(address);
//                                    if ((Boolean) opts.get("--sync")) {
//                                        writeEntry(fh, address, entry);
//                                    } else {
//                                        CompletableFuture.runAsync(() -> {
//                                            try {
//                                                writeEntry(fh, address, entry);
//                                            } catch (Exception e) {
//                                                log.error("Disk_write[{}]: Exception", address, e);
//                                            }
//                                        });
//                                    }
//                                } else {
//                                    throw new Exception("overwrite");
//                                }
//                                log.info("Disk_write[{}]: Written to disk.", address);
//                            } catch (Exception e) {
//                                log.error("Disk_write[{}]: Exception", address, e);
//                                throw new RuntimeException(e);
//                            }
//                        }
//                    }
//
//                    @Override
//                    public void delete(Long aLong, LogUnitEntry logUnitEntry, RemovalCause removalCause) {
//                        // never need to delete
//                    }
//                })

//       streamCache = Caffeine.newBuilder()
//                .maximumSize(Utils.getOption(opts, "--stream-cache", Long.class, 5L))
//                .writer(new CacheWriter<UUID, RangeSet<Long>>() {
//                    @Override
//                    public void write(UUID streamID, RangeSet<Long> entry) {
//                        if (prefix != null) {
//                            try {
//                                ByteBuf b = Unpooled.buffer();
//                                Set<Range<Long>> rs = entry.asRanges();
//                                b.writeInt(rs.size());
//                                for (Range<Long> r : rs)
//                                {
//                                    Serializers
//                                            .getSerializer(Serializers.SerializerType.JAVA).serialize(r, b);
//                                }
//                                com.google.common.io.Files.write(b.array(), new File(prefix + File.pathSeparator +
//                                        "stream" + streamID.toString()));
//                            } catch (IOException ie) {
//                                log.error("IOException while writing stream range for stream {}", streamID);
//                            }
//                        }
//                    }
//
//                    @Override
//                    public void delete(UUID streamID, RangeSet<Long> entry, RemovalCause removalCause) {
//                        // never need to delete
//                    }
//                }).build(this::handleStreamRetrieval);

        // Hints are always in memory and never persisted.
        /*
        hintCache = Caffeine.newBuilder()
                .weakKeys()
                .build();
*/
        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

//    @SuppressWarnings("unchecked")
//    public RangeSet<Long> handleStreamRetrieval(UUID stream) {
//        if (prefix != null) {
//            Path p = FileSystems.getDefault().getPath(prefix + File.pathSeparator +
//                    "stream" + stream.toString());
//            try {
//                if (Files.exists(p)) {
//                    ByteBuf b = Unpooled.wrappedBuffer(Files.readAllBytes(p));
//                    RangeSet rs = TreeRangeSet.create();
//                    int ranges = b.readInt();
//                    for (int i = 0; i < ranges; i++)
//                    {
//                        Range r = (Range) Serializers
//                                .getSerializer(Serializers.SerializerType.JAVA).deserialize(b, null);
//                        rs.add(r);
//                    }
//                }
//            } catch (IOException ie) {
//                log.error("IO Exception reading from stream file {}", p);
//            }
//        }
//        return TreeRangeSet.create();
//    }
//
//    /** Retrieve the LogUnitEntry from disk, given an address.
//     *
//     * @param address   The address to retrieve the entry from.
//     * @return          The log unit entry to retrieve into the cache.
//     *                  This function should not care about trimmed addresses, as that is handled in
//     *                  the read() and write(). Any address that cannot be retrieved should be returned as
//     *                  unwritten (null).
//     */
//    public synchronized LogUnitEntry handleRetrieval(Long address) {
//        log.trace("Retrieve[{}]", address);
//        if (prefix == null)
//        {
//            log.trace("This is an in-memory log unit, but a load was requested.");
//            return null;
//        }
//        FileHandle fh = getChannelForAddress(address);
//        try {
//            log.info("Got header {}", readHeader(fh.getChannel()));
//            return readEntry(getChannelForAddress(address), address);
//        } catch (Exception e)
//        {
//            throw new RuntimeException(e);
//        }
//    }

    public synchronized void handleEviction(Long address, LogUnitEntry entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.buffer != null) {
            if (prefix == null) {
                log.warn("This is an in-memory log unit, data@{} will be trimmed and lost due to {}!", address, cause);
                trimRange.add(Range.closed(address, address));
            }
            // Free the internal buffer once the data has been evicted (in the case the server is not sync).
            entry.buffer.release();
        }
    }

    /** Service an incoming read request. */
    public void read(ReplexLogUnitReadRequestMsg msg, ChannelHandlerContext ctx, IServerRouter r)
    {
        log.trace("Read[{}, {}]", msg.getStreamID(), msg.getOffset());
        /*if (trimRange.contains (msg.getAddress()))
        {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.TRIMMED));
        }
        else
        {*/
        LogUnitEntry e = dataCache.getIfPresent(new Pair<UUID, Long>(msg.getStreamID(), msg.getOffset()));
        if (e == null)
        {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.EMPTY));
        }
        else if (e.isHole)
        {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.FILLED_HOLE));
        } else {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(e));
        }
        //}
    }

    /** Service an incoming ranged read request. */
    /*public void read(CorfuRangeMsg msg, ChannelHandlerContext ctx, IServerRouter r)
    {
        log.trace("ReadRange[{}]", msg.getRanges());
        Set<Long> total = new HashSet<>();
        for (Range<Long> range : msg.getRanges().asRanges())
        {
            total.addAll(Utils.discretizeRange(range));
        }

        Map<Long, LogUnitEntry> e = dataCache.getAll(total);
        Map<Long, LogUnitReadResponseMsg> o = new ConcurrentHashMap<>();
        e.entrySet().parallelStream()
                .forEach(rv -> o.put(rv.getKey(), new LogUnitReadResponseMsg(rv.getValue())));
        r.sendResponse(ctx, msg, new LogUnitReadRangeResponseMsg(o));
    }*/

    /** Service an incoming write request. */
    public void write(ReplexLogUnitWriteMsg msg, ChannelHandlerContext ctx, IServerRouter r)
    {
        log.trace("Write for {} replex address", msg.getStreamPairs().size());
        // TODO: Check for Replex trims.
        //TODO: locking of trimRange.
        /*if (trimRange.contains (msg.getAddress()))
        {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_TRIMMED));
        }
        else {*/
        for (UUID streamID : msg.getStreamPairs().keySet()) {
            LogUnitEntry e = new LogUnitEntry(msg.getData(), msg.getMetadataMap(), false);
            e.getBuffer().retain();
            try {
                dataCache.put(new Pair(streamID, msg.getStreamPairs().get(streamID)), e);
            } catch (Exception ex) {
                // If a single one of the stream writes is an overwrite, then the whole thing gets aborted.
                // We don't have to worry about removing the writes from the cache, because the commit bit will never be
                // true.
                // TODO: Have a callback that acts on the Overwrite and set bits to aborted / false.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OVERWRITE));
                e.getBuffer().release();
                return;
            }
        }
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OK));
        //}
    }

//    public void runGC()
//    {
//        Thread.currentThread().setName("LogUnit-GC");
//        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
//                .setOptions(x -> x.setSentinelReference(running))
//                .setOptions(x -> x.setRetryInterval(60_000));
//
//        gcRetry = (IntervalAndSentinelRetry) retry;
//
//        retry.runForever();
//    }
//
//    @SuppressWarnings("unchecked")
//    public boolean handleGC()
//    {
//        log.info("Garbage collector starting...");
//        long freedEntries = 0;
//
//        log.trace("Trim range is {}", trimRange);
//
//        /* Pick a non-compacted region or just scan the cache */
//        Map<Long, LogUnitEntry> map = dataCache.asMap();
//        SortedSet<Long> addresses = new TreeSet<>(map.keySet());
//        for (long address : addresses)
//        {
//            LogUnitEntry buffer = dataCache.getIfPresent(address);
//            if (buffer != null)
//            {
//                Set<UUID> streams = buffer.getStreams();
//                // this is a normal entry
//                if (streams.size() > 0) {
//                    boolean trimmable = true;
//                    for (java.util.UUID stream : streams)
//                    {
//                        Long trimMark = trimMap.getOrDefault(stream, null);
//                        // if the stream has not been trimmed, or has not been trimmed to this point
//                        if (trimMark == null || address > trimMark) {
//                            trimmable = false;
//                            break;
//                        }
//                        // it is not trimmable.
//                    }
//                    if (trimmable) {
//                        log.trace("Trimming entry at {}", address);
//                        trimEntry(address, streams, buffer);
//                        freedEntries++;
//                    }
//                }
//                else {
//                    //this is an entry which belongs in all streams
//                }
//            }
//        }
//
//        log.info("Garbage collection pass complete. Freed {} entries", freedEntries);
//        return true;
//    }
//
//    public void trimEntry(long address, Set<java.util.UUID> streams, LogUnitEntry entry)
//    {
//        // Add this entry to the trimmed range map.
//        trimRange.add(Range.closed(address, address));
//        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
//        // assuming the entry is back by disk
//        dataCache.invalidate(address);
//        //and free any references the buffer might have
//        if (entry.getBuffer() != null)
//        {
//            entry.getBuffer().release();
//        }
//    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        // Clean up any file locks.
        if (channelMap != null) {
            channelMap.entrySet().parallelStream()
                    .forEach(f -> {
                        try {
                            f.getValue().getLock().release();
                        } catch (IOException ie) {
                            log.warn("Error releasing lock for channel {}", f.getKey());
                        }
                    });
        }
    }
}
