#include <string.h>
#include <raids/redis_cmd_db.h>

/* redis commands copied from https://redis.io/commands
 * attributes derived from redis command: 'command info name'
 */

namespace rai {
namespace ds {

CommandDB cmd_db[] = {
{ "NONE", "", "nil" },

#define CLUSTER_CNT 3
  /* cluster */
{ "CLUSTER", "[ADDSLOTS|COUNT-FAILURE|COUNTKEYSINSLOT|DELSLOTS|"
           "FAILOVER|FORGET|GETKEYSINSLOT|INFO|KEYSLOT|MEET|"
           "NODES|REPLICATE|RESET|SAVECONFIG|SET-CONFIG-EPOCH|"
           "SETSLOT|SLAVES|SLOTS] ; Cluster cmds",
  "[\"cluster\",-2,['admin'],0,0,0]"},
{ "READONLY", "; Enables readonly",
  "[\"readonly\",1,['fast'],0,0,0]" },
{ "READWRITE", "; Enables readwrite",
  "[\"readwrite\",1,['fast'],0,0,0]" },

#define CONNECTION_CNT 6
  /* connection */
{ "AUTH", "password ; Authenticate",
  "[\"auth\",2,['noscript','loading','stale','fast'],0,0,0]" },
{ "ECHO", "[msg] ; Echo",
  "[\"echo\",2,['fast'],0,0,0]" },
{ "PING", "[msg] ; Ping",
  "[\"ping\",-1,['stale','fast'],0,0,0]" },
{ "QUIT", "; Close",
  "[\"quit\",-1,['admin','loading','stale'],0,0,0]" },
{ "SELECT", "idx ; Change DB",
  "[\"select\",2,['loading','fast'],0,0,0]" },
{ "SWAPDB", "idx idx2 ; Swap two DBs",
  "[\"swapdb\",3,['write','fast'],0,0,0]" },

#define GEO_CNT 6
  /* geo */
{ "GEOADD", "key long lat mem [long lat mem ...] ; Add geo item",
  "[\"geoadd\",-5,['write','denyoom'],1,1,1]" },
{ "GEOHASH", "key mem [mem ...] ; Get geo hash strings",
  "[\"geohash\",-2,['readonly'],1,1,1]" },
{ "GEOPOS", "key [mem ...] ; Get positions of members",
  "[\"geopos\",-2,['readonly'],1,1,1]" },
{ "GEODIST", "key mem1 mem2 [unit] ; Get distance",
  "[\"geodist\",-4,['readonly'],1,1,1]" },
{ "GEORADIUS", "key long lat rad m|km|ft|mi [WITHCOORD] [WITHDIST]"
            "[WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]"
            "; Get members within geo spatual info",
  "[\"georadius\",-6,['write','movablekeys'],1,1,1]" },
{ "GEORADIUSBYMEMBER", "key mem (georadis opts) ; by mem",
  "[\"georadiusbymember\",-5,['write','movablekeys'],1,1,1]" },
/*
 * [\"georadiusbymember_ro\",-5,['readonly','movablekeys'],1,1,1]
 * [\"georadius_ro\",-6,['readonly','movablekeys'],1,1,1]
 */
#define HASH_CNT 22
  /* hashes */
{ "HAPPEND", "key field val [val ...]; Append value to a field",
  "[\"happend\",-4,['write','denyoom','fast'],1,1,1]" },
{ "HDEL", "key field [field ...] ; Delete fields",
  "[\"hdel\",-3,['write','fast'],1,1,1]" },
{ "HDIFF", "key [key ...] ; Subtract hashes",
  "[\"hdiff\",-2,['readonly','sort_for_script'],1,-1,1]" },
{ "HDIFFSTORE", "dest key [key ...] ; Subtract hashes and store",
  "[\"hdiffstore\",-3,['write','denyoom'],1,-1,1]" },
{ "HEXISTS", "key field ; Test if field exists",
  "[\"hexists\",3,['readonly','fast'],1,1,1]" },

{ "HGET", "key field ; Get value of field",
  "[\"hget\",3,['readonly','fast'],1,1,1]" },
{ "HGETALL", "key ; Get all the fields and values",
  "[\"hgetall\",2,['readonly'],1,1,1]" },
{ "HINCRBY", "key field int ; Incr field",
  "[\"hincrby\",4,['write','denyoom','fast'],1,1,1]" },
{ "HINCRBYFLOAT", "key field float ; Incr field by float",
  "[\"hincrbyfloat\",4,['write','denyoom','fast'],1,1,1]" },
{ "HINTER", "key [key ...] ; Intersect hashes",
  "[\"hinter\",-2,['readonly','sort_for_script'],1,-1,1]" },

{ "HINTERSTORE", "key [key ...] ; Intersect hashes and store",
  "[\"hinterstore\",-3,['write','denyoom'],1,-1,1]" },
{ "HKEYS", "key ; Get all the fields",
  "[\"hkeys\",2,['readonly','sort_for_script'],1,1,1]" },
{ "HLEN", "key ; Get number of fields",
  "[\"hlen\",2,['readonly','fast'],1,1,1]" },
{ "HMGET", "key field [field ...] ; Get multiple fields",
  "[\"hmget\",-3,['readonly','fast'],1,1,1]" },
{ "HMSET", "key field val [field val ...] ; Set multiple fields",
  "[\"hmset\",-4,['write','denyoom','fast'],1,1,1]" },

{ "HSET", "key field val ; Set the value of a field",
  "[\"hset\",-4,['write','denyoom','fast'],1,1,1]" },
{ "HSETNX", "key field val ; Set field value if ! exist",
  "[\"hsetnx\",4,['write','denyoom','fast'],1,1,1]" },
{ "HSTRLEN", "key field ; Get string length of field",
  "[\"hstrlen\",3,['readonly','fast'],1,1,1]" },
{ "HVALS", "key ; Get all field values",
  "[\"hvals\",2,['readonly','sort_for_script'],1,1,1]" },
{ "HSCAN", "key cursor [MATCH pat]; Iterate fields",
  "[\"hscan\",-3,['readonly','random'],1,1,1]" },

{ "HUNION", "key [key ...] ; Union hashes",
  "[\"hunion\",-2,['readonly','sort_for_script'],1,-1,1]" },
{ "HUNIONSTORE", "dest key [key ...] ; Union hashes and store",
  "[\"hunionstore\",-3,['write','denyoom'],1,-1,1]" },

#define HYPERLOGLOG_CNT 3
  /* hyperloglog */
{ "PFADD", "key elem [elem ...] ; Add elems",
  "[\"pfadd\",-2,['write','denyoom','fast'],1,1,1]" },
{ "PFCOUNT", "key [key ...] ; Get approximate cardinality",
  "[\"pfcount\",-2,['readonly'],1,-1,1]" },
{ "PFMERGE", "dkey skey [skey ...] ; Merge multiple logs",
  "[\"pfmerge\",-2,['write','denyoom'],1,-1,1]" },
/*
 * [\"pfdebug\",-3,['write'],0,0,0]
 * [\"pfselftest\",1,['admin'],0,0,0]
 */
#define KEY_CNT 24
  /* keys */
{ "DEL", "key [key ...] ; Delete key(s)",
  "[\"del\",-2,['write'],1,-1,1]" },
{ "DUMP", "key ; Serialize value",
  "[\"dump\",2,['readonly'],1,1,1]" },
{ "EXISTS", "key [key ...] ; Test exists",
  "[\"exists\",-2,['readonly','fast'],1,-1,1]" },
{ "EXPIRE", "key secs ; Set expire secs",
  "[\"expire\",3,['write','fast'],1,1,1]" },
{ "EXPIREAT", "key stamp ; Set expire at time",
  "[\"expireat\",3,['write','fast'],1,1,1]" },

{ "KEYS", "pattern ; Find all keys matching pattern",
  "[\"keys\",2,['readonly','sort_for_script'],0,0,0]" },
{ "MIGRATE", "host port key ; Atomically transfer key",
  "[\"migrate\",-6,['write','movablekeys'],0,0,0]" },
{ "MOVE", "key db ; Move key to another db",
  "[\"move\",3,['write','fast'],1,1,1]" },
{ "OBJECT", "subcmd [args] ; Inspect object",
  "[\"object\",-2,['readonly'],2,2,2]" },
{ "PERSIST", "key ; Remove expiration time",
  "[\"persist\",2,['write','fast'],1,1,1]" },

{ "PEXPIRE", "key ms ; Set expire ttl in millisecs",
  "[\"pexpire\",3,['write','fast'],1,1,1]" },
{ "PEXPIREAT", "key ms ; Set expire at ms stamp",
  "[\"pexpireat\",3,['write','fast'],1,1,1]" },
{ "PTTL", "key ; Get expire ttl in ms",
  "[\"pttl\",2,['readonly','fast'],1,1,1]" },
{ "RANDOMKEY", "; Get a random key",
  "[\"randomkey\",1,['readonly','random'],0,0,0]" },
{ "RENAME", "key newkey ; Rename key",
  "[\"rename\",3,['write'],1,2,1]" },

{ "RENAMENX", "key newkey ; Rename key if new key doesn't exists",
  "[\"renamenx\",3,['write','fast'],1,2,1]" },
{ "RESTORE", "key ttl val ; Restore key using dump format",
  "[\"restore\",-4,['write','denyoom'],1,1,1]" },
{ "SORT", "key [BY pat] [LIMIT off cnt] [GET pat] [ASC|DESC] [ALPHA]"
           "[STORE dest] ; Return sorted list, set or sorted set at key",
  "[\"sort\",-2,['write','denyoom','movablekeys'],1,1,1]" },
{ "TOUCH", "key [key ...] ; Set last access time of key",
  "[\"touch\",-2,['write','fast'],1,1,1]" },
{ "TTL", "key ; Get time to live",
  "[\"ttl\",2,['readonly','fast'],1,1,1]" },

{ "TYPE", "key ; Get the type of a key",
  "[\"type\",2,['readonly','fast'],1,1,1]" },
{ "UNLINK", "key [key ...] ; non blocking delete",
  "[\"unlink\",-2,['write','fast'],1,-1,1]" },
{ "WAIT", "numslave timeout ; wait for replication of write",
  "[\"wait\",3,['noscript'],0,0,0]" },
{ "SCAN", "curs [MATCH pat] [COUNT cnt] ; Iterate over key space",
  "[\"scan\",-2,['readonly','random'],0,0,0]" },
  
#define LIST_CNT 17
  /* lists */
{ "BLPOP", "key [key ...] timeout ; Block and get the first val",
  "[\"blpop\",-3,['write','noscript'],1,-2,1]" },
{ "BRPOP", "key [key ...] timeout ; Block and get the last val",
  "[\"brpop\",-3,['write','noscript'],1,-2,1]" },
{ "BRPOPLPUSH", "src dest ; Block and pop src, push dest",
  "[\"brpoplpush\",4,['write','denyoom','noscript'],1,2,1]" },
{ "LINDEX", "key idx ; Get list val by index",
  "[\"lindex\",3,['readonly'],1,1,1]" },
{ "LINSERT", "key BEFORE|AFTER piv val ; Insert val using pivot",
  "[\"linsert\",5,['write','denyoom'],1,1,1]" },

{ "LLEN", "key ; Get list length",
  "[\"llen\",2,['readonly','fast'],1,1,1]" },
{ "LPOP", "key ; Pop first val",
  "[\"lpop\",2,['write','fast'],1,1,1]" },
{ "LPUSH", "key val [val ..] ; Prepend val(s)",
  "[\"lpush\",-3,['write','denyoom','fast'],1,1,1]" },
{ "LPUSHX", "key val [val ..] ; Prepend val(s) if list exists",
  "[\"lpushx\",-3,['write','denyoom','fast'],1,1,1]" },
{ "LRANGE", "key start stop ; Get range of vals",
  "[\"lrange\",4,['readonly'],1,1,1]" },

{ "LREM", "key count value ; Remeove vals",
  "[\"lrem\",4,['write'],1,1,1]" },
{ "LSET", "key idx value ; Set value by index",
  "[\"lset\",4,['write','denyoom'],1,1,1]" },
{ "LTRIM", "key start stop ; Trim list to range",
  "[\"ltrim\",4,['write'],1,1,1]" },
{ "RPOP", "key ; Pop last val",
  "[\"rpop\",2,['write','fast'],1,1,1]" },
{ "RPOPLPUSH", "src dest ; Pop src, push dest",
  "[\"rpoplpush\",3,['write','denyoom'],1,2,1]" },

{ "RPUSH", "key [val ...] ; Append val to list",
  "[\"rpush\",-3,['write','denyoom','fast'],1,1,1]" },
{ "RPUSHX", "key [val ...] ; Append val to list, if it exists",
  "[\"rpushx\",-3,['write','denyoom','fast'],1,1,1]" },

#define PUBSUB_CNT 6
  /* pub/sub */
{ "PSUBSCRIBE", "pat [pat ...] ; Listen for msgs",
  "[\"psubscribe\",-2,['pubsub','noscript','loading','stale'],0,0,0]" },
{ "PUBSUB", "subcmd [arg ...] ; State of pub/sub",
  "[\"pubsub\",-2,['pubsub','random','loading','stale'],0,0,0]" },
{ "PUBLISH", "chan msg ; Publish msg to channel",
  "[\"publish\",3,['pubsub','loading','stale','fast'],0,0,0]" },
{ "PUNSUBSCRIBE", "[pat ...] ; Stop listening",
  "[\"punsubscribe\",-1,['pubsub','noscript','loading','stale'],0,0,0]" },
{ "SUBSCRIBE", "chan [chan ...] ; Subscribe channel(s)",
  "[\"subscribe\",-2,['pubsub','noscript','loading','stale'],0,0,0]" },

{ "UNSUBSCRIBE", "[chan ...] ; Unsubscribe channel(s)",
  "[\"unsubscribe\",-1,['pubsub','noscript','loading','stale'],0,0,0]" },

#define SCRIPT_CNT 3
  /* scripting */
{ "EVAL", "script num key [key ...] ; Execute a Lua script",
  "[\"eval\",-3,['noscript','movablekeys'],0,0,0]" },
{ "EVALSHA", "sha1 num key [key ...] ; Serverside Lua script",
  "[\"evalsha\",-3,['noscript','movablekeys'],0,0,0]" },
{ "SCRIPT", "[DEBUG|EXISTS|FLUSH|KILL|LOAD] ; Script ops",
  "[\"script\",-2,['noscript'],0,0,0]" },

#define SERVER_CNT 20
  /* server */
{ "BGREWRITEAOF", "; Asynchronously write append only file (AOF)",
  "[\"bgrewriteaof\",1,['admin'],0,0,0]" },
{ "BGSAVE", "; Async save dataset",
  "[\"bgsave\",-1,['admin'],0,0,0]" },
{ "CLIENT", "[KILL|LIST|GETNAME|PAUSE|REPLY|SETNAME] ; client ops",
  "[\"client\",-2,['admin','noscript'],0,0,0]" },
{ "COMMAND", "[COUNT|GETKEYS|INFO] ; Get command details",
  "[\"command\",-1,['loading','stale'],0,0,0]" },
{ "CONFIG", "[GET|REWRITE|SET|RESETSTAT] ; Get config details",
  "[\"config\",-2,['admin','loading','stale'],0,0,0]" },

{ "DBSIZE", "; Get number of keys",
  "[\"dbsize\",1,['readonly','fast'],0,0,0]" },
{ "DEBUG", "OBJECT key ; Get debug info about key",
  "[\"debug\",-1,['admin','noscript'],0,0,0]" },
{ "FLUSHALL", "[ASYNC] ; Remove keys from all dbs",
  "[\"flushall\",-1,['write'],0,0,0]" },
{ "FLUSHDB", "[ASYNC] ; Remove keys from selected db",
  "[\"flushdb\",-1,['write'],0,0,0]" },
{ "INFO", "[section] ; Get info and stats",
  "[\"info\",-1,['loading','stale'],0,0,0]" },

{ "LASTSAVE", "; Get the unix timestamp of the last save",
  "[\"lastsave\",1,['random','fast'],0,0,0]" },
{ "MEMORY", "[DOCTOR|HELP|MALLOC-STATS|PURGE|STATS|USAGE] ; Mem stats",
  "[\"memory\",-2,['readonly'],0,0,0]" },
{ "MONITOR", "; Listen for requests received by server",
  "[\"monitor\",1,['admin','noscript'],0,0,0]" },
{ "ROLE", "; What is role of the instance for replication",
  "[\"role\",1,['noscript','loading','stale'],0,0,0]" },
{ "SAVE", "; Synchronously save",
  "[\"save\",1,['admin','noscript'],0,0,0]" },

{ "SHUTDOWN", "[nosave|save] ; Shutdown server",
  "[\"shutdown\",-1,['admin','loading','stale'],0,0,0]" },
{ "SLAVEOF", "host port ; Make server a slave",
  "[\"slaveof\",3,['admin','noscript','stale'],0,0,0]" },
{ "SLOWLOG", "subcmd [arg] ; Manage slow queries log",
  "[\"slowlog\",-2,['admin'],0,0,0]" },
{ "SYNC", "; Internal cmd for replication",
  "[\"sync\",1,['readonly','admin','noscript'],0,0,0]" },
{ "TIME", "; Get server time",
  "[\"time\",1,['random','fast'],0,0,0]" },

#define SET_CNT 15
  /* sets */
{ "SADD", "key mem [mem ...] ; Add member(s) to set",
  "[\"sadd\",-3,['write','denyoom','fast'],1,1,1]" },
{ "SCARD", "key ; Get number of members",
  "[\"scard\",2,['readonly','fast'],1,1,1]" },
{ "SDIFF", "key [key ...] ; Subtract sets",
  "[\"sdiff\",-2,['readonly','sort_for_script'],1,-1,1]" },
{ "SDIFFSTORE", "dest key [key ...] ; Subtract sets and store",
  "[\"sdiffstore\",-3,['write','denyoom'],1,-1,1]" },
{ "SINTER", "key [key ...] ; Intersect sets",
  "[\"sinter\",-2,['readonly','sort_for_script'],1,-1,1]" },

{ "SINTERSTORE", "key [key ...] ; Intersect sets and store",
  "[\"sinterstore\",-3,['write','denyoom'],1,-1,1]" },
{ "SISMEMBER", "key mem ; Test membership",
  "[\"sismember\",3,['readonly','fast'],1,1,1]" },
{ "SMEMBERS", "key ; Get all the members in a set",
  "[\"smembers\",2,['readonly','sort_for_script'],1,1,1]" },
{ "SMOVE", "src dest mem ; Move a member to another set",
  "[\"smove\",4,['write','fast'],1,2,1]" },
{ "SPOP", "key [count] ; Remove random members",
  "[\"spop\",-2,['write','random','fast'],1,1,1]" },

{ "SRANDMEMBER", "key [count] ; Get random members",
  "[\"srandmember\",-2,['readonly','random'],1,1,1]" },
{ "SREM", "key mem [mem ...] ; Remove member(s)",
  "[\"srem\",-3,['write','fast'],1,1,1]" },
{ "SUNION", "key [key ...] ; Union sets",
  "[\"sunion\",-2,['readonly','sort_for_script'],1,-1,1]" },
{ "SUNIONSTORE", "dest key [key ...] ; Union sets and store",
  "[\"sunionstore\",-3,['write','denyoom'],1,-1,1]" },
{ "SSCAN", "key curs [MATCH pat] [COUNT cnt]; Iterate over set",
  "[\"sscan\",-3,['readonly','random'],1,1,1]" },

#define SORTED_SET_CNT 21
  /* sorted sets */
{ "ZADD", "key [NX|XX] [CH] [INCR] score mem ; Add mem w/score",
  "[\"zadd\",-4,['write','denyoom','fast'],1,1,1]" },
{ "ZCARD", "key ; Get number of members",
  "[\"zcard\",2,['readonly','fast'],1,1,1]" },
{ "ZCOUNT", "key min max ; Get number of members within bounds",
  "[\"zcount\",4,['readonly','fast'],1,1,1]" },
{ "ZINCRBY", "key incr mem ; Incr score of mem",
  "[\"zincrby\",4,['write','denyoom','fast'],1,1,1]" },
{ "ZINTERSTORE", "dest num key [key ...] WEIGHTS w [w ...]"
               "[AGGREGATE SUM|MIN|MAX] ; Intersect sets and store",
  "[\"zinterstore\",-4,['write','denyoom','movablekeys'],0,0,0]" },

{ "ZLEXCOUNT", "key min max ; Count members in a set within rang",
  "[\"zlexcount\",4,['readonly','fast'],1,1,1]" },
{ "ZRANGE", "key start stop [WITHSCORES] ; Get range of members",
  "[\"zrange\",-4,['readonly'],1,1,1]" },
{ "ZRANGEBYLEX", "key min max [LIMIT off cnt] ; Get lex range",
  "[\"zrangebylex\",-4,['readonly'],1,1,1]" },
{ "ZREVRANGEBYLEX", "key min max [LIMIT off cnt] ; Reverse",
  "[\"zrevrangebylex\",-4,['readonly'],1,1,1]" },
{ "ZRANGEBYSCORE", "key min max [WITHSCORES] ; Get by score",
  "[\"zrangebyscore\",-4,['readonly'],1,1,1]" },

{ "ZRANK", "key mem ; Get index of member",
  "[\"zrank\",3,['readonly','fast'],1,1,1]" },
{ "ZREM", "key mem [mem ...] ; Remove member(s)",
  "[\"zrem\",-3,['write','fast'],1,1,1]" },
{ "ZREMRANGEBYLEX", "key min max ; Remove by lex range",
  "[\"zremrangebylex\",4,['write'],1,1,1]" },
{ "ZREMRANGEBYRANK", "key start stop ; Remove by index",
  "[\"zremrangebyrank\",4,['write'],1,1,1]" },
{ "ZREMRANGEBYSCORE", "key start stop ; Remove by score",
  "[\"zremrangebyscore\",4,['write'],1,1,1]" },

{ "ZREVRANGE", "key start stop [WITHSCORES] ; Get rev range",
  "[\"zrevrange\",-4,['readonly'],1,1,1]" },
{ "ZREVRANGEBYSCORE", "key min max [WITHSCORES] [LIMIT ]",
  "[\"zrevrangebyscore\",-4,['readonly'],1,1,1]" },
{ "ZREVRANK", "key mem ; Get inverse rank index",
  "[\"zrevrank\",3,['readonly','fast'],1,1,1]" },
{ "ZSCORE", "key mem ; Get score of member",
  "[\"zscore\",3,['readonly','fast'],1,1,1]" },
{ "ZUNIONSTORE", "dest num key [key ...] WEIGHTS [] AGGR []",
  "[\"zunionstore\",-4,['write','denyoom','movablekeys'],0,0,0]" },

{ "ZSCAN", "key curs [MATCH pat] [COUNT cnt]; Iterate over set",
  "[\"zscan\",-3,['readonly','random'],1,1,1]" },

#define STRING_CNT 24
  /* strings */
{ "APPEND", "key value ; Append value to key",
  "[\"append\",3,['write','denyoom'],1,1,1]" },
{ "BITCOUNT", "key [start end] ; Count set bits in string",
  "[\"bitcount\",-2,['readonly'],1,1,1]" },
{ "BITFIELD", "key [GET type off] ; Bitfld int ops on string",
  "[\"bitfield\",-2,['write','denyoom'],1,1,1]" },
{ "BITOP", "op dkey key ; Bitwise op between strings",
  "[\"bitop\",-4,['write','denyoom'],2,-1,1]" },
{ "BITPOS", "key bit [start end] ; Find first bit set or clear",
  "[\"bitpos\",-3,['readonly'],1,1,1]" },

{ "DECR", "key ; Decrement int val",
  "[\"decr\",2,['write','denyoom','fast'],1,1,1]" },
{ "DECRBY", "key int ; Decrement int by amt",
  "[\"decrby\",3,['write','denyoom','fast'],1,1,1]" },
{ "GET", "key ; Get value",
  "[\"get\",2,['readonly','fast'],1,1,1]" },
{ "GETBIT", "key off ; Get bit val at offset in string",
  "[\"getbit\",3,['readonly','fast'],1,1,1]" },
{ "GETRANGE", "key start end ; Get a substring of the val",
  "[\"getrange\",4,['readonly'],1,1,1]" },

{ "GETSET", "key val ; Swap val with old val and return old",
  "[\"getset\",3,['write','denyoom'],1,1,1]" },
{ "INCR", "key ; incr int val",
  "[\"incr\",2,['write','denyoom','fast'],1,1,1]" },
{ "INCRBY", "key int ; incr int by amt",
  "[\"incrby\",3,['write','denyoom','fast'],1,1,1]" },
{ "INCRBYFLOAT", "key float ; incr float by amt",
  "[\"incrbyfloat\",3,['write','denyoom','fast'],1,1,1]" },
{ "MGET", "key [key ...] ; Get values of multiple keys",
  "[\"mget\",-2,['readonly','fast','multi_key_array'],1,-1,1]" },

{ "MSET", "key val [key val ...] ; Set values of multiple keys",
  "[\"mset\",-3,['write','denyoom'],1,-1,2]" },
{ "MSETNX", "key val [key val ...] ; Set values of keys if ! exist",
  "[\"msetnx\",-3,['write','denyoom'],1,-1,2]" },
{ "PSETEX", "key ms val ; Set val and expiration of key",
  "[\"psetex\",4,['write','denyoom'],1,1,1]" },
{ "SET", "key val [EX secs] [PX ms] [NX|XX] ; Set key val & expire",
  "[\"set\",-3,['write','denyoom'],1,1,1]" },
{ "SETBIT", "key off val ; Set or clear bit of str val at offset",
  "[\"setbit\",4,['write','denyoom'],1,1,1]" },

{ "SETEX", "key secs val ; Set value and expiration",
  "[\"setex\",4,['write','denyoom'],1,1,1]" },
{ "SETNX", "key val ; Set value if key ! exist",
  "[\"setnx\",3,['write','denyoom','fast'],1,1,1]" },
{ "SETRANGE", "key off val ; Overwrite range of key at offset",
  "[\"setrange\",4,['write','denyoom'],1,1,1]" },
{ "STRLEN", "key ; Get length of value",
  "[\"strlen\",2,['readonly','fast'],1,1,1]" },

#define TRANSACTION_CNT 5
  /* transactions */
{ "DISCARD", "; Discard cmds issued after multi",
  "[\"discard\",1,['noscript','fast'],0,0,0]" },
{ "EXEC", "; Execute all commands after multi",
  "[\"exec\",1,['noscript','skip_monitor'],0,0,0]" },
{ "MULTI", "; Multi start transaction",
  "[\"multi\",1,['noscript','fast'],0,0,0]" },
{ "UNWATCH", "; Forget watched keys",
  "[\"unwatch\",1,['noscript','fast'],0,0,0]" },
{ "WATCH", "key [key ...] ; Watch keys to determine multi/exec blk",
  "[\"watch\",-2,['noscript','fast'],1,-1,1]" },

#define STREAM_CNT 12
  /* streams -- https://redis.io/topics/streams-intro */
{ "XADD", "key entry-id [fld ...] ; Add to stream at key, entry-id",
  "[\"xadd\",-5,['write','denyoom','fast'],1,1,1]" },
{ "XLEN", "key ; Number of items in a stream",
  "[\"xlen\",2,['readonly','fast'],1,1,1]" },
{ "XRANGE", "key start end ; Get items in range",
  "[\"xrange\",-4,['readonly'],1,1,1]" },
{ "XREVRANGE", "key start end ; Get items in range in reverse",
  "[\"xrevrange\",-4,['readonly'],1,1,1]" },
{ "XREAD", "[COUNT N] [BLOCK N] STREAMS key ; Listen for new items",
  "[\"xread\",-3,['readonly','noscript','movablekeys'],1,1,1]" },

{ "XREADGROUP", "key group ; Read groups of streams",
  "[\"xreadgroup\",-3,['write','noscript','movablekeys'],1,1,1]" },
{ "XGROUP", "CREATE key group ; Create a group of streams",
  "[\"xgroup\",-2,['write','denyoom'],2,2,1]" },
{ "XACK", "key group entry-id ; Remove up to entry-id",
  "[\"xack\",-3,['write','fast'],1,1,1]" },
{ "XPENDING", "key group ; Get the unread entry-ids",
  "[\"xpending\",-3,['readonly'],1,1,1]" },
{ "XCLAIM", "key group consumer ; Recover consumer stream",
  "[\"xclaim\",-5,['write','fast'],1,1,1]" },

{ "XINFO", "key ; Get info for a stream",
  "[\"xinfo\",-2,['readonly'],2,2,1]" },
{ "XDEL", "key entry-id ; Delete a stream entry",
  "[\"xdel\",-2,['write','fast'],1,1,1]" }
};

const size_t cmd_db_cnt = sizeof( cmd_db ) / sizeof( cmd_db[ 0 ] );

const char *cmd_flag[] = {
  "write",   "readonly",    "denyoom",      "admin",
  "pubsub",  "noscript",    "random",       "sort_for_script",
  "loading", "stale",       "skip_monitor", "asking",
  "fast",    "movablekeys", "multi_key_array" };

const size_t cmd_flag_cnt = sizeof( cmd_flag ) /
                            sizeof( cmd_flag[ 0 ] );

}
}

