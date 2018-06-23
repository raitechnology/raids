#ifndef __rai_raids__redis_cmd_h__
#define __rai_raids__redis_cmd_h__

#include <raikv/key_hash.h>

namespace rai {
namespace ds {

enum RedisCatg {
  NO_CATG                =  0,
  CLUSTER_CATG           =  1,
  CONNECTION_CATG        =  2,
  GEO_CATG               =  3,
  HASH_CATG              =  4,
  HYPERLOGLOG_CATG       =  5,
  KEY_CATG               =  6,
  LIST_CATG              =  7,
  PUBSUB_CATG            =  8,
  SCRIPT_CATG            =  9,
  SERVER_CATG            = 10,
  SET_CATG               = 11,
  SORTED_SET_CATG        = 12,
  STRING_CATG            = 13,
  TRANSACTION_CATG       = 14,
  STREAM_CATG            = 15
};

enum RedisCmd {
  NO_CMD                 =   0,
  /* CLUSTER */
  CLUSTER_CMD            =   1,
  READONLY_CMD           =   2,
  READWRITE_CMD          =   3,
  /* CONNECTION */
  AUTH_CMD               =   4,
  ECHO_CMD               =   5,
  PING_CMD               =   6,
  QUIT_CMD               =   7,
  SELECT_CMD             =   8,
  SWAPDB_CMD             =   9,
  /* GEO */
  GEOADD_CMD             =  10,
  GEOHASH_CMD            =  11,
  GEOPOS_CMD             =  12,
  GEODIST_CMD            =  13,
  GEORADIUS_CMD          =  14,
  GEORADIUSBYMEMBER_CMD  =  15,
  /* HASH */
  HDEL_CMD               =  16,
  HEXISTS_CMD            =  17,
  HGET_CMD               =  18,
  HGETALL_CMD            =  19,
  HINCRBY_CMD            =  20,
  HINCRBYFLOAT_CMD       =  21,
  HKEYS_CMD              =  22,
  HLEN_CMD               =  23,
  HMGET_CMD              =  24,
  HMSET_CMD              =  25,
  HSET_CMD               =  26,
  HSETNX_CMD             =  27,
  HSTRLEN_CMD            =  28,
  HVALS_CMD              =  29,
  HSCAN_CMD              =  30,
  /* HYPERLOGLOG */
  PFADD_CMD              =  31,
  PFCOUNT_CMD            =  32,
  PFMERGE_CMD            =  33,
  /* KEY */
  DEL_CMD                =  34,
  DUMP_CMD               =  35,
  EXISTS_CMD             =  36,
  EXPIRE_CMD             =  37,
  EXPIREAT_CMD           =  38,
  KEYS_CMD               =  39,
  MIGRATE_CMD            =  40,
  MOVE_CMD               =  41,
  OBJECT_CMD             =  42,
  PERSIST_CMD            =  43,
  PEXPIRE_CMD            =  44,
  PEXPIREAT_CMD          =  45,
  PTTL_CMD               =  46,
  RANDOMKEY_CMD          =  47,
  RENAME_CMD             =  48,
  RENAMENX_CMD           =  49,
  RESTORE_CMD            =  50,
  SORT_CMD               =  51,
  TOUCH_CMD              =  52,
  TTL_CMD                =  53,
  TYPE_CMD               =  54,
  UNLINK_CMD             =  55,
  WAIT_CMD               =  56,
  SCAN_CMD               =  57,
  /* LIST */
  BLPOP_CMD              =  58,
  BRPOP_CMD              =  59,
  BRPOPLPUSH_CMD         =  60,
  LINDEX_CMD             =  61,
  LINSERT_CMD            =  62,
  LLEN_CMD               =  63,
  LPOP_CMD               =  64,
  LPUSH_CMD              =  65,
  LPUSHX_CMD             =  66,
  LRANGE_CMD             =  67,
  LREM_CMD               =  68,
  LSET_CMD               =  69,
  LTRIM_CMD              =  70,
  RPOP_CMD               =  71,
  RPOPLPUSH_CMD          =  72,
  RPUSH_CMD              =  73,
  RPUSHX_CMD             =  74,
  /* PUBSUB */
  PSUBSCRIBE_CMD         =  75,
  PUBSUB_CMD             =  76,
  PUBLISH_CMD            =  77,
  PUNSUBSCRIBE_CMD       =  78,
  SUBSCRIBE_CMD          =  79,
  UNSUBSCRIBE_CMD        =  80,
  /* SCRIPT */
  EVAL_CMD               =  81,
  EVALSHA_CMD            =  82,
  SCRIPT_CMD             =  83,
  /* SERVER */
  BGREWRITEAOF_CMD       =  84,
  BGSAVE_CMD             =  85,
  CLIENT_CMD             =  86,
  COMMAND_CMD            =  87,
  CONFIG_CMD             =  88,
  DBSIZE_CMD             =  89,
  DEBUG_CMD              =  90,
  FLUSHALL_CMD           =  91,
  FLUSHDB_CMD            =  92,
  INFO_CMD               =  93,
  LASTSAVE_CMD           =  94,
  MEMORY_CMD             =  95,
  MONITOR_CMD            =  96,
  ROLE_CMD               =  97,
  SAVE_CMD               =  98,
  SHUTDOWN_CMD           =  99,
  SLAVEOF_CMD            = 100,
  SLOWLOG_CMD            = 101,
  SYNC_CMD               = 102,
  TIME_CMD               = 103,
  /* SET */
  SADD_CMD               = 104,
  SCARD_CMD              = 105,
  SDIFF_CMD              = 106,
  SDIFFSTORE_CMD         = 107,
  SINTER_CMD             = 108,
  SINTERSTORE_CMD        = 109,
  SISMEMBER_CMD          = 110,
  SMEMBERS_CMD           = 111,
  SMOVE_CMD              = 112,
  SPOP_CMD               = 113,
  SRANDMEMBER_CMD        = 114,
  SREM_CMD               = 115,
  SUNION_CMD             = 116,
  SUNIONSTORE_CMD        = 117,
  SSCAN_CMD              = 118,
  /* SORTED_SET */
  ZADD_CMD               = 119,
  ZCARD_CMD              = 120,
  ZCOUNT_CMD             = 121,
  ZINCRBY_CMD            = 122,
  ZINTERSTORE_CMD        = 123,
  ZLEXCOUNT_CMD          = 124,
  ZRANGE_CMD             = 125,
  ZRANGEBYLEX_CMD        = 126,
  ZREVRANGEBYLEX_CMD     = 127,
  ZRANGEBYSCORE_CMD      = 128,
  ZRANK_CMD              = 129,
  ZREM_CMD               = 130,
  ZREMRANGEBYLEX_CMD     = 131,
  ZREMRANGEBYRANK_CMD    = 132,
  ZREMRANGEBYSCORE_CMD   = 133,
  ZREVRANGE_CMD          = 134,
  ZREVRANGEBYSCORE_CMD   = 135,
  ZREVRANK_CMD           = 136,
  ZSCORE_CMD             = 137,
  ZUNIONSTORE_CMD        = 138,
  ZSCAN_CMD              = 139,
  /* STRING */
  APPEND_CMD             = 140,
  BITCOUNT_CMD           = 141,
  BITFIELD_CMD           = 142,
  BITOP_CMD              = 143,
  BITPOS_CMD             = 144,
  DECR_CMD               = 145,
  DECRBY_CMD             = 146,
  GET_CMD                = 147,
  GETBIT_CMD             = 148,
  GETRANGE_CMD           = 149,
  GETSET_CMD             = 150,
  INCR_CMD               = 151,
  INCRBY_CMD             = 152,
  INCRBYFLOAT_CMD        = 153,
  MGET_CMD               = 154,
  MSET_CMD               = 155,
  MSETNX_CMD             = 156,
  PSETEX_CMD             = 157,
  SET_CMD                = 158,
  SETBIT_CMD             = 159,
  SETEX_CMD              = 160,
  SETNX_CMD              = 161,
  SETRANGE_CMD           = 162,
  STRLEN_CMD             = 163,
  /* TRANSACTION */
  DISCARD_CMD            = 164,
  EXEC_CMD               = 165,
  MULTI_CMD              = 166,
  UNWATCH_CMD            = 167,
  WATCH_CMD              = 168,
  /* STREAM */
  XADD_CMD               = 169,
  XLEN_CMD               = 170,
  XRANGE_CMD             = 171,
  XREVRANGE_CMD          = 172,
  XREAD_CMD              = 173,
  XREADGROUP_CMD         = 174,
  XGROUP_CMD             = 175,
  XACK_CMD               = 176,
  XPENDING_CMD           = 177,
  XCLAIM_CMD             = 178,
  XINFO_CMD              = 179,
  XDEL_CMD               = 180
};

static const size_t REDIS_CATG_COUNT = 16,
                    REDIS_CMD_COUNT  = 181;

static inline void
get_cmd_arity( RedisCmd cmd,  int &arity,  int &first,  int &last,  int &step ) {
  /* Arity of commands indexed by cmd */
  static const uint16_t redis_cmd_arity[] = {
    0,0xe,0x1,0x1,0x2,0x2,0xf,0xf,0x2,0x3,0x111b,0x111e,0x111e,0x111c,0x111a,0x111b,0x111d,0x1113,0x1113,0x1112,0x1114,0x1114,0x1112,0x1112,0x111d,0x111c,0x111c,0x1114,0x1113,0x1112,0x111d,0x111e,0x1f1e,0x1f1e,0x1f1e,0x1112,0x1f1e,0x1113,0x1113,0x2,0xa,0x1113,0x222e,0x1112,0x1113,0x1113,0x1112,0x1,0x1213,0x1213,0x111c,0x111e,0x111e,0x1112,0x1112,0x1f1e,0x3,0xe,0x1e1d,0x1e1d,0x1214,0x1113,0x1115,0x1112,0x1112,0x111d,0x111d,0x1114,0x1114,0x1114,0x1114,0x1112,0xf,0x111d,0x111d,0xe,0xe,0x3,0xf,0xe,0xf,0xd,0xd,0xe,0x1,0xf,0xe,0xf,0xe,0x1,0xf,0xf,0xf,0xf,0x1,0xe,0x1,0x1,0x1,0xf,0x3,0xe,0x1,0x1,0x111d,0x1112,0x1f1e,0x1f1d,0x1f1e,0x1f1d,0x1113,0x1112,0x1214,0x111e,0x111e,0x111d,0x1f1e,0x1f1d,0x111d,0x111c,0x1112,0x1114,0x1114,0xc,0x1114,0x111c,0x111c,0x111c,0x111c,0x1113,0x111d,0x1114,0x1114,0x1114,0x111c,0x111c,0x1113,0x1113,0xc,0x111d,0x1113,0x111e,0x111e,0x1f2c,0x111d,0x1112,0x1113,0x1112,0x1113,0x1114,0x1113,0x1112,0x1113,0x1113,0x1f1e,0x2f1d,0x2f1d,0x1114,0x111d,0x1114,0x1114,0x1113,0x1114,0x1112,0x1,0x1,0x1,0x1,0x1f1e,0x111b,0x1112,0x111c,0x111c,0x111d,0x111d,0x122e,0x111d,0x111d,0x111b,0x122e,0x111e};
  union {
    struct {
      int arity : 4, first : 4, last : 4, step : 4;
    } b;
    uint16_t val;
  } u;
  u.val = redis_cmd_arity[ cmd ];
  arity = u.b.arity; first = u.b.first;
  last  = u.b.last;  step  = u.b.step;
}

/* Flags enum:  used to test flags below ( 1 << CMD_FAST_FLAG )*/
enum RedisCmdFlag {
  CMD_WRITE_FLAG           =  0,
  CMD_READONLY_FLAG        =  1,
  CMD_DENYOOM_FLAG         =  2,
  CMD_ADMIN_FLAG           =  3,
  CMD_PUBSUB_FLAG          =  4,
  CMD_NOSCRIPT_FLAG        =  5,
  CMD_RANDOM_FLAG          =  6,
  CMD_SORT_FOR_SCRIPT_FLAG =  7,
  CMD_LOADING_FLAG         =  8,
  CMD_STALE_FLAG           =  9,
  CMD_SKIP_MONITOR_FLAG    = 10,
  CMD_ASKING_FLAG          = 11,
  CMD_FAST_FLAG            = 12,
  CMD_MOVABLEKEYS_FLAG     = 13,
  CMD_MULTI_KEY_ARRAY_FLAG = 14,
  CMD_MAX_FLAG             = 15
};

static inline uint16_t
get_cmd_flag_mask( RedisCmd cmd ) {
  /* Bit mask of flags indexed by cmd */
  static const uint16_t redis_cmd_flags[] = {
    0,0x8,0x1000,0x1000,0x1320,0x1000,0x1200,0x308,0x1100,0x1001,0x5,0x2,0x2,0x2,0x2001,0x2001,0x1001,0x1002,0x1002,0x2,0x1005,0x1005,0x82,0x1002,0x1002,0x1005,0x1005,0x1005,0x1002,0x82,0x42,0x1005,0x2,0x5,0x1,0x2,0x1002,0x1001,0x1001,0x82,0x2001,0x1001,0x2,0x1001,0x1001,0x1001,0x1002,0x42,0x1,0x1001,0x5,0x2005,0x1001,0x1002,0x1002,0x1001,0x20,0x42,0x21,0x21,0x25,0x2,0x5,0x1002,0x1001,0x1005,0x1005,0x2,0x1,0x5,0x1,0x1001,0x328,0x1005,0x1005,0x330,0x350,0x1310,0x330,0x330,0x330,0x2020,0x2020,0x20,0x8,0x8,0x28,0x300,0x308,0x1002,0x28,0x1,0x1,0x300,0x1040,0x2,0x28,0x320,0x28,0x308,0x228,0x8,0x2a,0x1040,0x1005,0x1002,0x82,0x5,0x82,0x5,0x1002,0x82,0x1001,0x1041,0x42,0x1001,0x82,0x5,0x42,0x1005,0x1002,0x1002,0x1005,0x2005,0x1002,0x2,0x2,0x2,0x2,0x1002,0x1001,0x1,0x1,0x1,0x2,0x2,0x1002,0x1002,0x2005,0x42,0x5,0x2,0x5,0x5,0x2,0x1005,0x1005,0x1002,0x1002,0x2,0x5,0x1005,0x1005,0x1005,0x5002,0x5,0x5,0x5,0x5,0x5,0x5,0x1005,0x5,0x1002,0x1020,0x420,0x1020,0x1020,0x1020,0x1005,0x1002,0x2,0x2,0x2022,0x2021,0x5,0x1001,0x2,0x1001,0x2,0x1001};
  return redis_cmd_flags[ cmd ];
}

static inline bool
test_cmd_flag( RedisCmd cmd,  RedisCmdFlag fl ) {
  return ( get_cmd_flag_mask( cmd ) & ( 1U << (int) fl ) ) != 0;
}

static inline uint16_t
test_cmd_mask( uint16_t mask,  RedisCmdFlag fl ) {
  return mask & ( 1U << (int) fl );
}

static inline RedisCatg
get_cmd_category( RedisCmd cmd ) {
  static const uint32_t catg[] = {
    0x22221110U,0x33333322U,0x44444444U,0x54444444U,0x66666655U,0x66666666U,0x66666666U,0x77777766U,0x77777777U,0x88888777U,0xaaaa9998U,0xaaaaaaaaU,0xaaaaaaaaU,0xbbbbbbbbU,0xcbbbbbbbU,0xccccccccU,0xccccccccU,0xddddccccU,0xddddddddU,0xddddddddU,0xeeeeddddU,0xfffffffeU,0x000fffffU };
  uint32_t x = (uint32_t) cmd;
  x = ( catg[ x >> 3 ] >> ( 4 * ( x & 7 ) ) ) & 0xf;
  return (RedisCatg) x;
}

/* Generated ht[] is a perfect hash, but does not strcmp() cmd */
static inline RedisCmd
get_redis_cmd( const char *cmd,  size_t len ) {
  static const uint8_t ht[] = {
    45,0,0,0,0,0,0,0,0,0,86,0,0,0,0,0,0,98,137,0,0,0,0,30,0,84,0,0,0,0,0,0,0,0,0,0,122,0,0,0,0,0,0,0,0,127,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,115,0,0,0,0,134,0,40,0,168,151,0,128,170,0,80,0,0,0,136,0,0,0,0,162,0,43,0,0,0,0,0,0,71,0,0,0,0,49,0,75,0,0,41,0,0,0,0,0,0,0,0,0,0,0,0,72,0,0,0,0,0,0,0,0,0,0,63,0,0,0,0,34,0,0,0,0,120,0,0,0,18,0,0,178,0,1,0,0,91,0,0,0,0,0,0,165,0,0,169,50,0,0,0,0,0,0,39,0,97,114,0,0,11,105,0,0,0,159,0,0,0,0,0,0,0,152,103,0,0,0,0,0,0,0,0,0,129,64,0,0,0,0,0,0,0,0,0,0,0,0,0,0,138,0,0,0,0,0,0,13,0,0,0,0,0,0,0,0,0,0,0,0,0,144,0,116,0,0,0,69,0,0,0,0,0,0,0,53,0,0,29,0,26,38,0,0,0,0,0,111,0,107,0,0,0,0,0,0,7,10,0,0,166,0,0,133,0,0,0,0,0,0,0,121,0,0,135,6,0,0,0,0,0,0,0,0,0,0,73,0,167,0,0,0,0,0,0,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,94,0,8,0,173,0,0,160,0,0,163,0,9,0,59,22,0,0,130,0,99,0,76,0,0,0,0,89,0,0,0,0,0,0,23,0,51,0,0,60,0,0,0,0,0,36,0,0,0,0,0,0,0,0,0,0,126,74,0,0,0,0,0,0,0,0,95,0,0,0,0,156,0,0,88,0,0,0,0,14,0,0,0,0,0,0,35,0,0,0,0,0,0,0,0,0,0,101,0,44,0,0,81,0,0,0,0,0,0,0,0,0,0,0,92,0,176,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,70,124,0,0,0,0,0,0,0,0,0,0,67,0,57,46,0,0,0,37,0,0,0,87,0,0,0,0,0,0,0,58,0,0,0,27,0,0,0,123,0,0,0,0,175,0,0,0,0,0,55,0,0,0,0,0,0,0,0,0,0,0,32,106,0,0,0,0,0,0,0,0,66,0,0,78,0,0,0,0,172,0,0,0,0,0,19,0,0,68,24,0,42,0,0,0,0,0,0,0,0,131,0,0,0,0,0,12,0,0,0,0,0,0,0,0,0,0,155,0,0,174,0,0,21,145,0,0,0,0,142,0,0,0,0,0,0,93,0,5,0,0,0,0,0,0,0,0,0,0,125,150,0,0,0,0,0,0,0,0,0,0,161,0,179,0,0,0,104,180,0,0,0,0,0,65,0,0,0,0,0,0,0,0,0,0,102,0,0,0,0,0,0,0,0,0,0,0,0,139,0,0,0,0,118,0,0,0,143,54,0,171,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,79,0,0,158,0,0,0,0,100,0,0,0,0,0,0,0,0,0,17,0,117,0,0,113,0,0,0,0,0,0,108,0,0,0,0,0,0,0,96,0,0,0,0,0,0,0,0,0,0,56,0,0,0,28,0,157,0,0,0,0,0,0,0,4,0,0,0,0,0,0,0,0,0,164,0,0,0,0,0,0,2,20,0,0,0,0,0,0,0,77,0,0,0,0,0,0,0,0,119,0,0,3,0,112,0,0,0,0,0,0,0,0,0,141,0,0,48,0,140,0,0,0,0,132,0,0,0,0,0,0,16,0,0,0,0,0,0,0,0,0,0,0,62,0,0,0,0,147,0,52,0,0,0,0,0,0,153,0,0,0,0,0,85,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,47,0,0,0,0,0,0,0,149,0,0,0,0,0,177,0,90,0,0,0,0,0,0,0,0,0,0,0,0,0,0,82,0,0,0,146,0,0,0,0,0,0,0,0,0,0,0,0,0,110,0,0,0,0,0,31,0,25,0,0,0,0,0,0,0,33,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,83,0,0,0,154,0,0,0,0,0,61,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,109,0,0,0,0,0,0,0,0,0,0,148,0,0,0};
  static const uint8_t cmdlen[] = {
    0x60,0x87,0x33,0x33,0x55,0x65,0x65,0x8,0x63,0x63,0xb6,0x34,0x44,0x53,0x46,0x44,0x66,0x32,0x55,0x37,0x36,0x65,0x86,0x83,0x75,0x36,0x24,0x53,0x33,0x44,0x59,0x36,0x43,0x55,0x33,0x34,0x48,0x95,0x65,0x8b,0x3a,0x56,0x5b,0x65,0x55,0x74,0x36,0x57,0x36,0x73,0x66,0x33,0x43,0x94,0xa5,0x78,0x34,0x3a,0xa5,0x34,0x54,0xa6,0x58,0xda,0x4c,0xd3,0xfe,0xf8,0x57,0x4a,0x75,0x47,0x35,0x25,0x75,0x35,0xa5,0x33,0x55,0x52,0x44,0x57,0x36,0x64,0x34,0x53,0x48,0x59,0x73,0x45};
  uint8_t c = ht[ kv_crc_c( cmd, len, 0x2169e7fb ) % 1024 ];
  uint8_t l = cmdlen[ c / 2 ] >> ( 4 * ( c & 1 ) );
  return ( ( l & 0xfU ) == ( ( len - 1 ) & 0xfU ) ) ? (RedisCmd) c : NO_CMD;
}

}
}
#endif
