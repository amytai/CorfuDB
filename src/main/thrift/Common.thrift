namespace java org.corfudb.infrastructure.thrift


enum ErrorCode {
	OK,
	ERR_OVERWRITE,
	ERR_TRIMMED,
	ERR_UNWRITTEN,
	ERR_BADPARAM,
	ERR_FULL,
	ERR_IO,
	OK_SKIP,
	ERR_STALEEPOCH,
	ERR_SUBLOG,
}

enum ExtntMarkType {	EX_EMPTY, EX_FILLED, EX_TRIMMED, EX_SKIP }

struct ExtntInfo {
	1: i64 metaFirstOff,
	2: i32 metaLength,
	3: ExtntMarkType flag=ExtntMarkType.EX_FILLED
}

struct UUID {
	1: i64 msb,
	2: i64 lsb
}

typedef binary MultiCommand

struct Hints {
	1: ErrorCode err,
	2: map<UUID, i64> nextMap,
	3: bool txDec,
	4: MultiCommand flatTxn,
}

typedef binary LogPayload
typedef list<i32> Epoch

struct ExtntWrap {
	1: ErrorCode err,
	2: ExtntInfo inf,
	3: list<LogPayload> ctnt
}

struct UnitServerHdr {
    1: Epoch epoch,
    2: i64 off,
    3: set<UUID> streamID,
}

struct StreamUnitServerHdr {
    1: Epoch epoch,
    2: i64 off,
    3: map<UUID, i64> streams,
}

struct WriteResult {
    1: ErrorCode code,
    2: binary data
}
	

