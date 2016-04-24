package org.corfudb.protocols.wireprotocol;

import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mwei on 9/18/15.
 */
public interface IMetadata {

    @RequiredArgsConstructor
    public enum LogUnitMetadataType {
        STREAM(0),
        RANK(1),
        STREAM_ADDRESS(2),
        BACKPOINTER_MAP(3),
        REPLEX_COMMIT(4),
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, LogUnitMetadataType> metadataTypeMap =
            Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                    .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));

    EnumMap<IMetadata.LogUnitMetadataType, Object> getMetadataMap();

    /** Get the streams that belong to this write.
     *
     * @return A set of streams that belong to this write.
     */
    @SuppressWarnings("unchecked")
    default Set<UUID> getStreams()
    {
        return (Set<UUID>) getMetadataMap().getOrDefault(
                LogUnitMetadataType.STREAM,
                Collections.EMPTY_SET);
    }

    /** Set the streams that belong to this write.
     *
     * @param streams The set of streams that will belong to this write.
     */
    default void setStreams(Set<UUID> streams)
    {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.STREAM, streams);
    }

    /** Get the rank of this write.
     *
     * @return The rank of this write.
     */
    @SuppressWarnings("unchecked")
    default Long getRank()
    {
        return (Long) getMetadataMap().getOrDefault(IMetadata.LogUnitMetadataType.RANK,
                0L);
    }

    /** Set the rank of this write.
     *
     * @param rank The rank of this write.
     */
    default void setRank(Long rank)
    {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.RANK, rank);
    }

    /** Get the logical stream addresses that belong to this write.
     *
     * @return A list of logical stream addresses that belong to this write.
     */
    @SuppressWarnings("unchecked")
    default List<Long> getLogicalAddresses()
    {
        return (List<Long>) getMetadataMap().getOrDefault(IMetadata.LogUnitMetadataType.STREAM_ADDRESS,
                Collections.EMPTY_LIST);
    }

    /** Set the logical stream addresses that belong to this write.
     *
     * @param streams The set of logical stream addresses that will belong to this write.
     */
    default void setLogicalAddresses(List<Long> streams)
    {
        getMetadataMap().put(IMetadata.LogUnitMetadataType.STREAM_ADDRESS, streams);
    }

    default void setBackpointerMap(Map<UUID, Long> backpointerMap)
    {
        getMetadataMap().put(LogUnitMetadataType.BACKPOINTER_MAP, backpointerMap);
    }

    @SuppressWarnings("unchecked")
    default Map<UUID, Long> getBackpointerMap()
    {
        return (Map<UUID, Long>) getMetadataMap().getOrDefault(LogUnitMetadataType.BACKPOINTER_MAP,
                Collections.EMPTY_MAP);
    }

    default void setReplexCommit(boolean commit) {
        getMetadataMap().put(LogUnitMetadataType.REPLEX_COMMIT, commit);
    }

    default boolean getReplexCommit() {
        return (boolean) getMetadataMap().getOrDefault(LogUnitMetadataType.REPLEX_COMMIT, false);
    }
}
