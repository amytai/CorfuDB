namespace java  org.corfudb.infrastructure.thrift

include "Common.thrift"

service RocksLogUnitService {

    Common.WriteResult write(1:Common.StreamUnitServerHdr hdr, 2:Common.LogPayload ctnt, 3:Common.ExtntMarkType et),
	
    Common.ErrorCode fix(1:Common.UnitServerHdr hdr),

    Common.ExtntWrap read(1:Common.UnitServerHdr hdr),
    
    Common.ErrorCode setCommit(1:Common.UnitServerHdr hdr, 2:bool commit),
	
    void sync(),
	
    i64 querytrim(),
	
    i64 queryck(),
	
    void ckpoint(1:Common.UnitServerHdr hdr),
		
    bool ping(),

    void reset(),

    void simulateFailure(1:bool fail, 2:i64 length),

    void setEpoch(1:i64 epoch),

    i64 highestAddress();
}
