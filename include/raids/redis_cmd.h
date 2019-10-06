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
  HAPPEND_CMD            =  16,
  HDEL_CMD               =  17,
  HDIFF_CMD              =  18,
  HDIFFSTORE_CMD         =  19,
  HEXISTS_CMD            =  20,
  HGET_CMD               =  21,
  HGETALL_CMD            =  22,
  HINCRBY_CMD            =  23,
  HINCRBYFLOAT_CMD       =  24,
  HINTER_CMD             =  25,
  HINTERSTORE_CMD        =  26,
  HKEYS_CMD              =  27,
  HLEN_CMD               =  28,
  HMGET_CMD              =  29,
  HMSET_CMD              =  30,
  HSET_CMD               =  31,
  HSETNX_CMD             =  32,
  HSTRLEN_CMD            =  33,
  HVALS_CMD              =  34,
  HSCAN_CMD              =  35,
  HUNION_CMD             =  36,
  HUNIONSTORE_CMD        =  37,
  /* HYPERLOGLOG */
  PFADD_CMD              =  38,
  PFCOUNT_CMD            =  39,
  PFMERGE_CMD            =  40,
  /* KEY */
  DEL_CMD                =  41,
  DUMP_CMD               =  42,
  EXISTS_CMD             =  43,
  EXPIRE_CMD             =  44,
  EXPIREAT_CMD           =  45,
  KEYS_CMD               =  46,
  MIGRATE_CMD            =  47,
  MOVE_CMD               =  48,
  OBJECT_CMD             =  49,
  PERSIST_CMD            =  50,
  PEXPIRE_CMD            =  51,
  PEXPIREAT_CMD          =  52,
  PTTL_CMD               =  53,
  RANDOMKEY_CMD          =  54,
  RENAME_CMD             =  55,
  RENAMENX_CMD           =  56,
  RESTORE_CMD            =  57,
  SORT_CMD               =  58,
  TOUCH_CMD              =  59,
  TTL_CMD                =  60,
  TYPE_CMD               =  61,
  UNLINK_CMD             =  62,
  WAIT_CMD               =  63,
  SCAN_CMD               =  64,
  /* LIST */
  BLPOP_CMD              =  65,
  BRPOP_CMD              =  66,
  BRPOPLPUSH_CMD         =  67,
  LINDEX_CMD             =  68,
  LINSERT_CMD            =  69,
  LLEN_CMD               =  70,
  LPOP_CMD               =  71,
  LPUSH_CMD              =  72,
  LPUSHX_CMD             =  73,
  LRANGE_CMD             =  74,
  LREM_CMD               =  75,
  LSET_CMD               =  76,
  LTRIM_CMD              =  77,
  RPOP_CMD               =  78,
  RPOPLPUSH_CMD          =  79,
  RPUSH_CMD              =  80,
  RPUSHX_CMD             =  81,
  /* PUBSUB */
  PSUBSCRIBE_CMD         =  82,
  PUBSUB_CMD             =  83,
  PUBLISH_CMD            =  84,
  PUNSUBSCRIBE_CMD       =  85,
  SUBSCRIBE_CMD          =  86,
  UNSUBSCRIBE_CMD        =  87,
  /* SCRIPT */
  EVAL_CMD               =  88,
  EVALSHA_CMD            =  89,
  SCRIPT_CMD             =  90,
  /* SERVER */
  BGREWRITEAOF_CMD       =  91,
  BGSAVE_CMD             =  92,
  CLIENT_CMD             =  93,
  COMMAND_CMD            =  94,
  CONFIG_CMD             =  95,
  DBSIZE_CMD             =  96,
  DEBUG_CMD              =  97,
  FLUSHALL_CMD           =  98,
  FLUSHDB_CMD            =  99,
  INFO_CMD               = 100,
  LASTSAVE_CMD           = 101,
  MEMORY_CMD             = 102,
  MONITOR_CMD            = 103,
  ROLE_CMD               = 104,
  SAVE_CMD               = 105,
  SHUTDOWN_CMD           = 106,
  SLAVEOF_CMD            = 107,
  SLOWLOG_CMD            = 108,
  SYNC_CMD               = 109,
  TIME_CMD               = 110,
  /* SET */
  SADD_CMD               = 111,
  SCARD_CMD              = 112,
  SDIFF_CMD              = 113,
  SDIFFSTORE_CMD         = 114,
  SINTER_CMD             = 115,
  SINTERSTORE_CMD        = 116,
  SISMEMBER_CMD          = 117,
  SMEMBERS_CMD           = 118,
  SMOVE_CMD              = 119,
  SPOP_CMD               = 120,
  SRANDMEMBER_CMD        = 121,
  SREM_CMD               = 122,
  SUNION_CMD             = 123,
  SUNIONSTORE_CMD        = 124,
  SSCAN_CMD              = 125,
  /* SORTED_SET */
  ZADD_CMD               = 126,
  ZCARD_CMD              = 127,
  ZCOUNT_CMD             = 128,
  ZINCRBY_CMD            = 129,
  ZINTERSTORE_CMD        = 130,
  ZLEXCOUNT_CMD          = 131,
  ZRANGE_CMD             = 132,
  ZRANGEBYLEX_CMD        = 133,
  ZREVRANGEBYLEX_CMD     = 134,
  ZRANGEBYSCORE_CMD      = 135,
  ZRANK_CMD              = 136,
  ZREM_CMD               = 137,
  ZREMRANGEBYLEX_CMD     = 138,
  ZREMRANGEBYRANK_CMD    = 139,
  ZREMRANGEBYSCORE_CMD   = 140,
  ZREVRANGE_CMD          = 141,
  ZREVRANGEBYSCORE_CMD   = 142,
  ZREVRANK_CMD           = 143,
  ZSCORE_CMD             = 144,
  ZUNIONSTORE_CMD        = 145,
  ZSCAN_CMD              = 146,
  /* STRING */
  APPEND_CMD             = 147,
  BITCOUNT_CMD           = 148,
  BITFIELD_CMD           = 149,
  BITOP_CMD              = 150,
  BITPOS_CMD             = 151,
  DECR_CMD               = 152,
  DECRBY_CMD             = 153,
  GET_CMD                = 154,
  GETBIT_CMD             = 155,
  GETRANGE_CMD           = 156,
  GETSET_CMD             = 157,
  INCR_CMD               = 158,
  INCRBY_CMD             = 159,
  INCRBYFLOAT_CMD        = 160,
  MGET_CMD               = 161,
  MSET_CMD               = 162,
  MSETNX_CMD             = 163,
  PSETEX_CMD             = 164,
  SET_CMD                = 165,
  SETBIT_CMD             = 166,
  SETEX_CMD              = 167,
  SETNX_CMD              = 168,
  SETRANGE_CMD           = 169,
  STRLEN_CMD             = 170,
  /* TRANSACTION */
  DISCARD_CMD            = 171,
  EXEC_CMD               = 172,
  MULTI_CMD              = 173,
  UNWATCH_CMD            = 174,
  WATCH_CMD              = 175,
  /* STREAM */
  XADD_CMD               = 176,
  XLEN_CMD               = 177,
  XRANGE_CMD             = 178,
  XREVRANGE_CMD          = 179,
  XREAD_CMD              = 180,
  XREADGROUP_CMD         = 181,
  XGROUP_CMD             = 182,
  XACK_CMD               = 183,
  XPENDING_CMD           = 184,
  XCLAIM_CMD             = 185,
  XINFO_CMD              = 186,
  XDEL_CMD               = 187
};

static const size_t REDIS_CATG_COUNT = 16,
                    REDIS_CMD_COUNT  = 188;

static inline void
get_cmd_arity( RedisCmd cmd,  int16_t &arity,  int16_t &first,  int16_t &last,  int16_t &step ) {
  /* Arity of commands indexed by cmd */
  static const uint16_t redis_cmd_arity[] = {
    0,0xe,0x1,0x1,0x2,0x2,0xf,0xf,0x2,0x3,0x111b,0x111e,0x111e,0x111c,0x111a,0x111b,0x111c,0x111d,0x1f1e,0x1f1d,0x1113,0x1113,0x1112,0x1114,0x1114,0x1f1e,0x1f1d,0x1112,0x1112,0x111d,0x111c,0x111c,0x1114,0x1113,0x1112,0x111d,0x1f1e,0x1f1d,0x111e,0x1f1e,0x1f1e,0x1f1e,0x1112,0x1f1e,0x1113,0x1113,0x2,0xa,0x1113,0x222e,0x1112,0x1113,0x1113,0x1112,0x1,0x1213,0x1213,0x111c,0x111e,0x111e,0x1112,0x1112,0x1f1e,0x3,0xe,0x1e1d,0x1e1d,0x1214,0x1113,0x1115,0x1112,0x1112,0x111d,0x111d,0x1114,0x1114,0x1114,0x1114,0x1112,0x1213,0x111d,0x111d,0xe,0xe,0x3,0xf,0xe,0xf,0xd,0xd,0xe,0x1,0xf,0xe,0xf,0xe,0x1,0xf,0xf,0xf,0xf,0x1,0xe,0x1,0x1,0x1,0xf,0x3,0xe,0x1,0x1,0x111d,0x1112,0x1f1e,0x1f1d,0x1f1e,0x1f1d,0x1113,0x1112,0x1214,0x111e,0x111e,0x111d,0x1f1e,0x1f1d,0x111d,0x111c,0x1112,0x1114,0x1114,0xc,0x1114,0x111c,0x111c,0x111c,0x111c,0x1113,0x111d,0x1114,0x1114,0x1114,0x111c,0x111c,0x1113,0x1113,0xc,0x111d,0x1113,0x111e,0x111e,0x1f2c,0x111d,0x1112,0x1113,0x1112,0x1113,0x1114,0x1113,0x1112,0x1113,0x1113,0x1f1e,0x2f1d,0x2f1d,0x1114,0x111d,0x1114,0x1114,0x1113,0x1114,0x1112,0x1,0x1,0x1,0x1,0x1f1e,0x111b,0x1112,0x111c,0x111c,0x111d,0x111d,0x122e,0x111d,0x111d,0x111b,0x122e,0x111e};
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
  CMD_MAX_FLAG             = 14
};

static inline uint16_t
get_cmd_flag_mask( RedisCmd cmd ) {
  /* Bit mask of flags indexed by cmd */
  static const uint16_t redis_cmd_flags[] = {
    0,0x8,0x1000,0x1000,0x1320,0x1000,0x1200,0x308,0x1100,0x1001,0x5,0x2,0x2,0x2,0x2001,0x2001,0x1005,0x1001,0x82,0x5,0x1002,0x1002,0x2,0x1005,0x1005,0x82,0x5,0x82,0x1002,0x1002,0x1005,0x1005,0x1005,0x1002,0x82,0x42,0x82,0x5,0x1005,0x2,0x5,0x1,0x2,0x1002,0x1001,0x1001,0x82,0x2001,0x1001,0x2,0x1001,0x1001,0x1001,0x1002,0x42,0x1,0x1001,0x5,0x2005,0x1001,0x1002,0x1002,0x1001,0x20,0x42,0x21,0x21,0x25,0x2,0x5,0x1002,0x1001,0x1005,0x1005,0x2,0x1,0x5,0x1,0x1001,0x5,0x1005,0x1005,0x330,0x350,0x1310,0x330,0x330,0x330,0x2020,0x2020,0x20,0x8,0x8,0x28,0x300,0x308,0x1002,0x28,0x1,0x1,0x300,0x1040,0x2,0x28,0x320,0x28,0x308,0x228,0x8,0x2a,0x1040,0x1005,0x1002,0x82,0x5,0x82,0x5,0x1002,0x82,0x1001,0x1041,0x42,0x1001,0x82,0x5,0x42,0x1005,0x1002,0x1002,0x1005,0x2005,0x1002,0x2,0x2,0x2,0x2,0x1002,0x1001,0x1,0x1,0x1,0x2,0x2,0x1002,0x1002,0x2005,0x42,0x5,0x2,0x5,0x5,0x2,0x1005,0x1005,0x1002,0x1002,0x2,0x5,0x1005,0x1005,0x1005,0x1002,0x5,0x5,0x5,0x5,0x5,0x5,0x1005,0x5,0x1002,0x1020,0x420,0x1020,0x1020,0x1022,0x1005,0x1002,0x2,0x2,0x2022,0x2021,0x5,0x1001,0x2,0x1001,0x2,0x1001};
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
    0x22221110U,0x33333322U,0x44444444U,0x44444444U,0x55444444U,0x66666665U,0x66666666U,0x66666666U,0x77777776U,0x77777777U,0x88888877U,0xaaaaa999U,0xaaaaaaaaU,0xbaaaaaaaU,0xbbbbbbbbU,0xccbbbbbbU,0xccccccccU,0xccccccccU,0xdddddcccU,0xddddddddU,0xddddddddU,0xeeeeedddU,0xffffffffU,0x0000ffffU };
  uint32_t x = (uint32_t) cmd;
  x = ( catg[ x >> 3 ] >> ( 4 * ( x & 7 ) ) ) & 0xf;
  return (RedisCatg) x;
}

/* Generated ht[] is a perfect hash, but does not strcmp() cmd */
static inline RedisCmd
get_redis_cmd( const char *cmd,  size_t len ) {
  static const uint8_t ht[] = {
    0,0,0,0,0,0,100,0,0,0,152,0,0,0,185,0,181,0,0,162,0,0,0,0,0,0,0,0,3,0,0,0,159,0,0,0,0,111,187,40,166,0,0,124,0,0,0,0,138,0,0,0,0,168,0,0,5,0,0,186,0,0,0,0,0,0,0,0,0,89,0,0,0,0,0,0,0,0,0,0,0,0,0,0,56,0,113,0,0,0,0,19,0,0,0,0,0,0,0,143,0,0,169,0,0,0,0,0,0,29,0,0,151,0,123,0,0,0,75,0,0,0,0,0,0,0,0,0,120,0,0,0,0,0,0,0,0,0,0,0,0,117,0,144,0,0,0,0,0,0,0,93,0,0,0,0,0,0,0,0,0,0,0,0,0,24,0,0,0,98,63,0,0,0,0,69,0,0,0,0,0,0,0,0,0,0,0,0,54,0,0,0,0,0,0,0,0,0,72,0,0,0,0,0,0,0,0,0,0,0,0,0,85,0,0,109,0,0,0,0,0,0,146,0,0,0,0,125,0,0,0,150,0,0,171,0,0,0,0,0,0,0,23,0,0,0,0,0,0,0,84,0,0,0,61,0,0,0,0,0,95,0,0,103,0,0,0,163,102,0,0,116,0,0,139,0,59,81,33,0,0,0,0,17,0,0,0,179,0,0,0,0,0,0,0,0,0,107,0,0,0,0,0,0,45,0,0,20,0,0,118,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,106,0,0,0,0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,32,0,0,0,0,119,0,0,0,0,0,0,0,0,0,0,44,0,0,0,0,0,126,74,0,101,0,0,0,128,161,0,0,0,0,0,0,0,0,10,0,0,0,0,142,0,0,0,0,0,0,0,0,0,0,60,0,0,0,0,0,0,160,140,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,25,0,0,0,0,0,0,0,0,0,0,170,0,9,0,0,0,0,22,8,0,0,0,0,0,0,0,97,0,0,0,0,0,0,0,0,0,0,0,41,43,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,39,0,0,0,96,83,0,0,86,0,30,0,38,0,0,0,0,0,0,0,0,0,0,175,0,94,0,0,0,0,158,0,0,177,0,0,0,0,0,0,0,0,0,0,0,0,99,0,0,0,0,145,134,0,0,0,15,0,0,0,0,0,0,48,154,0,178,0,0,78,82,0,0,37,0,0,0,0,0,115,0,0,0,0,0,0,105,0,0,0,0,0,0,108,0,51,121,0,35,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,164,149,122,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,73,0,0,0,0,0,0,0,0,0,136,0,0,0,0,0,0,71,0,0,0,0,0,0,0,0,62,0,182,0,0,0,0,0,0,0,14,0,0,0,0,0,0,76,49,0,0,0,87,0,0,0,0,0,0,0,0,0,0,0,34,0,0,0,0,0,0,0,0,0,0,172,21,0,0,0,0,0,127,0,174,0,0,0,0,0,0,0,0,0,70,0,0,12,0,0,18,0,0,0,0,0,110,0,0,0,0,0,131,91,0,0,0,157,46,112,104,0,132,0,0,0,176,0,0,0,0,0,0,0,130,148,0,0,0,0,0,0,0,0,0,0,0,0,0,180,0,0,167,0,0,0,26,0,0,0,27,0,0,66,2,0,0,0,0,0,0,0,67,28,0,58,0,0,0,0,0,0,0,0,153,0,0,0,0,137,52,0,0,0,0,13,0,0,0,57,0,0,0,0,7,0,90,0,0,79,0,0,11,0,0,0,0,0,173,114,31,0,0,0,0,0,0,0,0,155,0,0,0,0,0,0,0,0,0,0,80,0,0,0,0,0,0,0,0,1,0,0,16,133,0,6,68,0,141,0,0,0,0,0,0,0,0,0,0,36,0,0,0,0,0,0,0,0,0,0,0,0,0,77,0,0,0,183,0,0,0,0,0,0,0,165,0,184,47,0,55,0,147,0,0,0,0,0,0,0,0,0,0,0,65,0,0,50,0,64,53,0,0,156,0,0,0,0,129,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,88,0,0,0,0,0,0,0,0,0,135,0,0,0,0,0,42,92,0};
  static const uint32_t hashes[] = {
    0,0x5a35a775,0xb70e7f20,0x81640c1c,0x5c3eb158,0xbe9e3c38,0x65b5737b,0x17bec348,0xc8bb51cc,0x6037b5c6,0x2c8d758c,0x510dfb50,0x940952dd,0xb9279f3f,0x5b1956a4,0x4ddba2b,0x61479778,0x4921691b,0xf29adee0,0x5e25945b,0x34a3f933,0x398836c8,0x17284dcb,0x289f8cf2,0x471e90a5,0x5cc57db9,0x77728318,0x4642b71c,0x904b2729,0xf5f9c06d,0x1b8519fd,0xd7f4ef58,0x30d56165,0x7ae47516,0x33652bc,0x85742a55,0x62e52789,0xbdc8823c,0xf997e9ff,0x94b56df3,0x90e53c27,0xb888c9e0,0xd1ad1ffd,0x65c901e1,0xff001975,0xf097dd30,0x2c54daf2,0xbdb00fa6,0x4ea98e32,0xb67752ac,0x5aec67b9,0x7c1ce652,0x580c273a,0xac4afbc,0x81c1f0bc,0x10dc6ba8,0x31232454,0x8bddc343,0x39736b2b,0xeb814114,0xe44ddd9c,0x44102cfe,0x468269a,0x3d8708aa,0xef6247bb,0xe460c3b6,0x3f05031f,0xffc7e328,0xf5701f7c,0xe0a45caf,0xeb6916da,0x3b337a91,0x9821b0c6,0x3e486280,0xe74fc97c,0x617b5076,0xacd6deab,0x2796f397,0xe056ba38,0x8a78074d,0x4203bb6c,0xf73bdd15,0x5d4ba639,0x80c281f8,0x4eff54fa,0x476e50d4,0x360115fb,0xc5bab6b0,0x6845dfed,0x8ad06c45,0x2e14334a,0xadc4deed,0x8f8357fe,0x939aa497,0x2b98c60c,0x51e49104,0xd5618df7,0x21cbe5d4,0x11e87ca9,0x4207f621,0xc4edf406,0x34e8d7e,0x38fc0d0c,0x4d22d507,0xcd1de6f4,0x7c2c8649,0xe3a23152,0xb7185529,0x5026ba50,0xb0af10d7,0xcb4b9ae6,0x96c7c425,0x41c962f3,0xf0ec0456,0xcd5c5357,0xc8739e42,0x3aa2350f,0x16bb6c8d,0xdd3ce136,0x26c9396a,0x3d131080,0x936f2a53,0x675b3a67,0xf653c472,0xf018342b,0x8702f0e3,0xbdc60d7b,0xbcbf06ce,0x5568d82,0x64c22fc4,0x1eda702,0xe2a36aec,0x4e3a6ef6,0xc811bb79,0x926a9227,0xab4afff7,0x3ad6728a,0x4c5af339,0xdfba2430,0xfb7f8112,0x1650b5a4,0xd598737e,0xc74c5591,0x1ab82c63,0xee0b88f,0xcb57a626,0x7a7494de,0x302d6faa,0xd7bad303,0x8c129a66,0xdcd9fce7,0x91ece070,0xb4ef800a,0xc9628b34,0xc8219633,0x6bc31361,0x1f3af7bf,0x6dbe5ef1,0x36b38611,0x79f57420,0x528179a3,0x9fefad83,0x71937413,0x6d103d0b,0x392c2265,0x265d4fa3,0x193a1428,0xd6040b14,0x7ff76035,0xba517c66,0x2b8e8dc4,0x5b7cc8ea,0x913c4ac7,0x63e3df56,0x5c5692d0,0x34f2120a,0x2a12efa,0x792f9614,0x95b6ce35,0x3809a91f,0x6d403f11,0xc46a8810,0xd1e8c69c,0x26dfdb9b,0x4ab0b7a5,0x4005a80e,0x21d9f83b,0xa045d826};
  uint32_t k = kv_crc_c( cmd, len, 0xa28b04b0 );
  uint8_t  c = ht[ k % 1024 ];
  return hashes[ c ] == k ? (RedisCmd) c : NO_CMD;
}

}
}
#endif
