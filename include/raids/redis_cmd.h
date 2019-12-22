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
  ZPOPMIN_CMD            = 147,
  ZPOPMAX_CMD            = 148,
  BZPOPMIN_CMD           = 149,
  BZPOPMAX_CMD           = 150,
  /* STRING */
  APPEND_CMD             = 151,
  BITCOUNT_CMD           = 152,
  BITFIELD_CMD           = 153,
  BITOP_CMD              = 154,
  BITPOS_CMD             = 155,
  DECR_CMD               = 156,
  DECRBY_CMD             = 157,
  GET_CMD                = 158,
  GETBIT_CMD             = 159,
  GETRANGE_CMD           = 160,
  GETSET_CMD             = 161,
  INCR_CMD               = 162,
  INCRBY_CMD             = 163,
  INCRBYFLOAT_CMD        = 164,
  MGET_CMD               = 165,
  MSET_CMD               = 166,
  MSETNX_CMD             = 167,
  PSETEX_CMD             = 168,
  SET_CMD                = 169,
  SETBIT_CMD             = 170,
  SETEX_CMD              = 171,
  SETNX_CMD              = 172,
  SETRANGE_CMD           = 173,
  STRLEN_CMD             = 174,
  /* TRANSACTION */
  DISCARD_CMD            = 175,
  EXEC_CMD               = 176,
  MULTI_CMD              = 177,
  UNWATCH_CMD            = 178,
  WATCH_CMD              = 179,
  /* STREAM */
  XINFO_CMD              = 180,
  XADD_CMD               = 181,
  XTRIM_CMD              = 182,
  XDEL_CMD               = 183,
  XRANGE_CMD             = 184,
  XREVRANGE_CMD          = 185,
  XLEN_CMD               = 186,
  XREAD_CMD              = 187,
  XGROUP_CMD             = 188,
  XREADGROUP_CMD         = 189,
  XACK_CMD               = 190,
  XCLAIM_CMD             = 191,
  XPENDING_CMD           = 192
};

static const size_t REDIS_CATG_COUNT = 16,
                    REDIS_CMD_COUNT  = 193;

static inline void
get_cmd_arity( RedisCmd cmd,  int16_t &arity,  int16_t &first,  int16_t &last,  int16_t &step ) {
  /* Arity of commands indexed by cmd */
  static const uint16_t redis_cmd_arity[] = {
    0,0xe,0x1,0x1,0x2,0x2,0xf,0xf,0x2,0x3,0x111b,0x111e,0x111e,0x111c,0x111a,0x111b,0x111c,0x111d,0x1f1e,0x1f1d,0x1113,0x1113,0x1112,0x1114,0x1114,0x1f1e,0x1f1d,0x1112,0x1112,0x111d,0x111c,0x111c,0x1114,0x1113,0x1112,0x111d,0x1f1e,0x1f1d,0x111e,0x1f1e,0x1f1e,0x1f1e,0x1112,0x1f1e,0x1113,0x1113,0x2,0xa,0x1113,0x222e,0x1112,0x1113,0x1113,0x1112,0x1,0x1213,0x1213,0x111c,0x111e,0x111e,0x1112,0x1112,0x1f1e,0x3,0xe,0x1e1d,0x1e1d,0x1214,0x1113,0x1115,0x1112,0x1112,0x111d,0x111d,0x1114,0x1114,0x1114,0x1114,0x1112,0x1213,0x111d,0x111d,0xe,0xe,0x3,0xf,0xe,0xf,0xd,0xd,0xe,0x1,0xf,0xe,0xf,0xe,0x1,0xf,0xf,0xf,0xf,0x1,0xe,0x1,0x1,0x1,0xf,0x3,0xe,0x1,0x1,0x111d,0x1112,0x1f1e,0x1f1d,0x1f1e,0x1f1d,0x1113,0x1112,0x1214,0x111e,0x111e,0x111d,0x1f1e,0x1f1d,0x111d,0x111c,0x1112,0x1114,0x1114,0xc,0x1114,0x111c,0x111c,0x111c,0x111c,0x1113,0x111d,0x1114,0x1114,0x1114,0x111c,0x111c,0x1113,0x1113,0xc,0x111d,0x111e,0x111e,0x1e1d,0x1e1d,0x1113,0x111e,0x111e,0x1f2c,0x111d,0x1112,0x1113,0x1112,0x1113,0x1114,0x1113,0x1112,0x1113,0x1113,0x1f1e,0x2f1d,0x2f1d,0x1114,0x111d,0x1114,0x1114,0x1113,0x1114,0x1112,0x1,0x1,0x1,0x1,0x1f1e,0x122e,0x111b,0x111e,0x111e,0x111c,0x111c,0x1112,0x111d,0x122e,0x111d,0x111d,0x111b,0x111d};
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
    0,0x8,0x1000,0x1000,0x1320,0x1000,0x1200,0x308,0x1100,0x1001,0x5,0x2,0x2,0x2,0x2001,0x2001,0x1005,0x1001,0x82,0x5,0x1002,0x1002,0x2,0x1005,0x1005,0x82,0x5,0x82,0x1002,0x1002,0x1005,0x1005,0x1005,0x1002,0x82,0x42,0x82,0x5,0x1005,0x2,0x5,0x1,0x2,0x1002,0x1001,0x1001,0x82,0x2001,0x1001,0x2,0x1001,0x1001,0x1001,0x1002,0x42,0x1,0x1001,0x5,0x2005,0x1001,0x1002,0x1002,0x1001,0x20,0x42,0x21,0x21,0x25,0x2,0x5,0x1002,0x1001,0x1005,0x1005,0x2,0x1,0x5,0x1,0x1001,0x5,0x1005,0x1005,0x330,0x350,0x1310,0x330,0x330,0x330,0x2020,0x2020,0x20,0x8,0x8,0x28,0x300,0x308,0x1002,0x28,0x1,0x1,0x300,0x1040,0x2,0x28,0x320,0x28,0x308,0x228,0x8,0x2a,0x1040,0x1005,0x1002,0x82,0x5,0x82,0x5,0x1002,0x82,0x1001,0x1041,0x42,0x1001,0x82,0x5,0x42,0x1005,0x1002,0x1002,0x1005,0x2005,0x1002,0x2,0x2,0x2,0x2,0x1002,0x1001,0x1,0x1,0x1,0x2,0x2,0x1002,0x1002,0x2005,0x42,0x1001,0x1001,0x21,0x21,0x5,0x2,0x5,0x5,0x2,0x1005,0x1005,0x1002,0x1002,0x2,0x5,0x1005,0x1005,0x1005,0x1002,0x5,0x5,0x5,0x5,0x5,0x5,0x1005,0x5,0x1002,0x1020,0x420,0x1020,0x1020,0x1022,0x2,0x1005,0x1,0x1001,0x2,0x2,0x1002,0x2022,0x5,0x2021,0x1001,0x1001,0x2};
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
    0x22221110U,0x33333322U,0x44444444U,0x44444444U,0x55444444U,0x66666665U,0x66666666U,0x66666666U,0x77777776U,0x77777777U,0x88888877U,0xaaaaa999U,0xaaaaaaaaU,0xbaaaaaaaU,0xbbbbbbbbU,0xccbbbbbbU,0xccccccccU,0xccccccccU,0xdcccccccU,0xddddddddU,0xddddddddU,0xedddddddU,0xffffeeeeU,0xffffffffU,0x0000000fU };
  uint32_t x = (uint32_t) cmd;
  x = ( catg[ x >> 3 ] >> ( 4 * ( x & 7 ) ) ) & 0xf;
  return (RedisCatg) x;
}

/* Generated ht[] is a perfect hash, but does not strcmp() cmd */
static inline RedisCmd
get_redis_cmd( const char *cmd,  size_t len ) {
  static const uint8_t ht[] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,113,0,0,0,191,0,0,0,0,0,0,0,0,0,39,0,0,0,0,0,165,0,22,0,0,29,0,0,0,0,0,0,0,0,170,0,0,0,0,0,0,0,163,0,86,0,0,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,123,0,155,0,0,0,0,0,0,172,0,149,0,0,0,0,0,0,0,180,0,0,0,0,72,0,0,0,0,0,0,0,133,0,0,93,0,114,0,0,0,0,0,144,0,0,0,0,146,0,0,0,0,0,0,154,0,0,45,125,118,0,0,0,0,0,0,0,0,0,0,0,0,0,182,0,0,0,0,0,0,0,0,0,17,0,0,0,106,0,0,0,107,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,185,0,0,0,126,0,20,0,0,0,0,0,0,0,0,26,0,0,103,0,101,0,0,0,67,130,0,0,0,0,0,0,0,33,0,0,0,4,0,0,0,0,0,0,0,175,0,0,0,0,0,63,0,81,91,167,0,0,0,0,0,84,102,0,0,0,0,0,0,23,95,0,0,0,0,0,0,120,60,0,0,0,0,0,119,0,0,0,0,0,0,0,0,0,0,0,0,98,147,0,0,0,0,87,0,0,69,0,0,0,0,0,0,0,117,0,0,0,0,0,0,0,59,0,0,0,0,61,0,0,41,0,0,0,0,0,0,0,0,139,0,0,74,0,0,0,0,0,0,44,0,0,109,0,0,0,0,0,0,0,0,54,0,0,0,32,0,183,0,0,111,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,97,0,0,0,10,0,128,0,0,0,0,5,0,100,0,25,121,0,0,0,0,0,89,0,0,156,0,0,0,0,0,0,0,0,166,0,0,38,0,30,0,0,0,0,0,0,0,0,0,0,0,0,56,0,0,0,40,37,0,0,0,75,0,134,0,0,0,145,8,0,0,3,0,0,9,0,174,82,0,0,83,0,0,0,0,0,0,0,0,96,0,0,0,143,0,0,0,0,173,0,0,0,0,43,0,0,0,0,0,0,0,0,0,148,0,0,0,0,0,0,0,0,0,0,0,0,0,35,0,0,0,0,0,0,0,53,0,0,64,0,0,0,0,0,0,0,0,0,0,129,0,0,0,160,0,184,0,0,0,0,0,0,0,192,0,0,0,0,0,0,190,0,0,0,0,0,0,0,0,0,0,0,0,47,88,0,179,0,0,0,0,0,164,0,50,0,0,0,115,0,42,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,168,0,0,62,0,0,0,0,0,188,158,127,0,79,0,58,0,28,152,0,0,0,16,0,0,0,0,1,0,73,0,0,0,137,0,0,0,0,0,0,0,18,0,0,0,0,0,0,0,57,0,0,0,2,112,0,0,49,141,0,0,0,11,0,0,0,0,0,0,0,0,0,12,0,0,0,0,0,0,136,0,0,0,0,0,13,0,0,0,0,0,0,0,0,0,6,0,0,0,0,0,0,0,0,0,0,0,0,161,0,0,132,7,0,0,135,0,0,0,0,0,0,0,0,0,34,52,0,31,116,0,0,0,0,0,0,0,0,0,0,0,0,0,76,0,0,0,177,0,0,0,0,0,0,169,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,131,80,0,0,0,157,0,0,0,0,0,0,85,71,0,0,0,0,178,0,0,0,110,0,0,0,0,0,0,24,0,0,0,150,0,0,0,0,0,90,104,171,46,0,187,0,66,0,0,27,181,0,0,0,0,176,14,0,0,0,0,68,0,0,0,0,0,0,21,0,0,0,0,0,0,0,0,0,0,0,159,0,70,0,0,0,0,0,19,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,36,0,48,0,0,0,0,140,0,0,0,0,78,0,0,0,0,0,0,0,0,0,0,0,0,0,142,0,0,0,0,151,186,55,0,0,0,162,0,0,0,0,108,0,51,0,0,122,0,0,0,138,0,0,0,189,0,0,0,99,0,0,77,0,0,0,0,0,0,0,0,0,0,124,0,0,0,0,94,0,0,0,0,92,0,0,0,0,0,0,0,0,0,105,0,65,0,0,0,153,0,0,0,0,0,0,0,0,0,0};
  static const uint32_t hashes[] = {
    0,0xd377fe99,0x19f45ab3,0x32f9da,0x7720b8fe,0x9580359e,0x4eab7add,0x3ca0caee,0x5d8ee9d7,0xf5020ddd,0xb9b8cd97,0xd84fa2bc,0x13ceac6,0x3065c6d3,0xda4fa362,0x8b414043,0xe805ce94,0x623f60bd,0x765b92a7,0xbb880382,0xbde1a0df,0x12963f6e,0x9e6a1427,0xa1ddd51e,0xb4db7b47,0xc9f0c5a2,0xdcf614e8,0xc283fb5b,0xbb552e8f,0x71388c2a,0x9f4455ba,0xfceae6fe,0xa5e0d97e,0xf3a62cfa,0x87f71efb,0x1b56612,0xf7d09f92,0x164c15cc,0x7d56a5b8,0x1df7341f,0x19a765cb,0x3a589d5b,0xfab3165b,0xf0fcb9fa,0x6a35a16e,0x5e6df8a3,0x74ad354,0x34f2564a,0x65b78794,0x2342eab7,0xd3ae3e55,0xf55ebfbe,0xd95ad2fc,0x21daa61a,0x97057a,0x85e9d3b3,0x9fd901c7,0x29f9aaf,0x126d628d,0x6f400d53,0x669d8927,0x6f0e2558,0x915d9e81,0x1699010c,0xc47c4e1d,0x60a18ff1,0xbbc44f58,0x1a6a74f1,0x6045a767,0x69e60543,0xc0771f7c,0x102d7337,0x1ce0fc81,0xab7dda9b,0x727a7167,0x4a6559d0,0x87c8d70d,0xa357bfd0,0xcb48b39e,0xb2ef28b,0xc6c2f72b,0x620e650e,0xb8e631e0,0x15f739e3,0xc7bd0d16,0xb4abbb36,0xb757e03d,0x6e3e2140,0x435bd64b,0x39235a9,0xbb218b51,0x5e01350f,0x1ab6efe5,0x6af1c8c,0xa2da9fe0,0xc4d1291f,0x405435ec,0xa50aa993,0xbf12593a,0xcb45afcd,0xeff3fda0,0xadb4a8ed,0xadc9b517,0xc4608ceb,0xe603ef52,0x57328fef,0x4d5814c1,0x3e5a0cc5,0xd964e3bc,0x9bb11971,0xe0559340,0xbdd9cd83,0xc5082eb4,0x742d4811,0x28f1c48e,0x5d462659,0x9126a2ff,0x97ed994b,0x73c6c4a5,0xa208752d,0x160d1926,0x38ebbda3,0x4c4533c1,0x63667c69,0x5b9ca3db,0x3c3bca4,0x96d804dd,0x387e4a89,0x90633599,0xed807628,0xaa6930f2,0x63f59f2a,0xdb0fd6ed,0x63952c89,0xb4c98dd2,0xa9b2e2f1,0xbe173ecd,0x6744fa9f,0xf9193bc5,0x3dc65964,0x41afbf99,0x54ce86b8,0x90b35fac,0xb44209f0,0x9bd50094,0x60d331d6,0xfeb5d899,0x40898d3b,0xeb626e04,0x85cbf474,0x2e20174b,0xa518d7b1,0x7940f690,0x22e8bff5,0x5818b0a0,0x4d9586b,0x9ff189ac,0x5c57332f,0x4af1c288,0xfef6ab7a,0xb1c0d22c,0xf88be6ea,0x1dad8fb7,0xecc0cc3b,0xf905ee53,0xb4f1a425,0x5a8d7db5,0xf8258510,0xac199a7e,0xa48d1b18,0x8c0fac33,0x52c54753,0xfb362c72,0x14ab59f5,0xbebb35df,0xd23e9106,0xba224361,0xe7229311,0xd514cb3c,0xb0335e4d,0xa518b47c,0x29bf275c,0xcc3060b3,0x8b5bd180,0x83762e,0xb95f5cd9,0x52319fb2,0xe9817356,0x44dd7e87,0x21c71fc9,0xdc1d23d,0xd5301015,0xe44a9236};
  uint32_t k = kv_crc_c( cmd, len, 0x36fbdccd );
  uint8_t  c = ht[ k % 1024 ];
  return hashes[ c ] == k ? (RedisCmd) c : NO_CMD;
}

}
}
#endif
