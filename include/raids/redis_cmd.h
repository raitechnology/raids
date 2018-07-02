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
  static const uint32_t hashes[] = {
    0,0x77158c95,0x5413f711,0x430fdb26,0xe96aef00,0xbca6260,0xd0e12d23,0xa2ea9d10,0x68207551,0xc0ac915b,0x8c165111,0x7c2dd0b0,0x34927640,0x9407b4df,0x9972819e,0xa514953a,0xfc753743,0x1983d2d3,0x8cdc6890,0x3a08662b,0x5bfa712,0x18f54651,0x9d0b615e,0x251f7971,0x2eb0162f,0xc0cccfbf,0x62a0b100,0x904e45f8,0x57c45ef6,0xd87f84fe,0x5e3dfc17,0x22de3fbd,0xb9954613,0xbdc517c7,0xe0211887,0x64f941a5,0xc552257c,0x5f9b3de8,0x138a5501,0x990084aa,0x90902446,0xfbfdd06a,0x16ec7631,0x77cc4c59,0x513ccdb2,0x9a67f000,0xbf90f1e4,0x43aa2786,0xb0474f35,0xd23eac65,0xa6fde8a3,0x8c273573,0x30c89756,0xbce40cfb,0xf14472a6,0xa4f30207,0x88d356f2,0x5a3619e3,0x3f2915f4,0xe44cd55d,0xa404e976,0x55eb3be1,0xcd84774f,0x5e3d4882,0x8e6724c9,0x43686684,0x9ed3461d,0x47d4ede1,0xd42f0e2e,0x198280f3,0xfcdf25d5,0x5502e460,0x4813d077,0x994a6d2e,0x57a0f988,0x688ac67,0x2059a565,0x63df7f1a,0x18858620,0xf46ac2c1,0x38bc104e,0xdd1181b5,0xa7f047a5,0x8e8f17d7,0xf22f0819,0x2f187363,0x3301800a,0x6b8edec,0xf17fb599,0x75faa96a,0xfa823396,0xf2f5f498,0x6f27ddc1,0x71b9aa5e,0xe053054f,0x98672991,0x6002fee7,0x7849b8ac,0xc978d811,0xbfb963,0x9a387ec9,0x7d0691b0,0x5fb4e8f,0x7e1fc4be,0x23939a7d,0x9a80b4b1,0x2ba5d214,0x969f5909,0x68e8badf,0xc7a493f1,0xd4d0bbb7,0x3e216907,0xfd80ef28,0x88474ed8,0x6e698cad,0xd20f643f,0x56c8e0ef,0xd1e92d5,0x5c4b26a1,0x8925323,0x67f6d08c,0xa5cda91f,0x49e20424,0xfceb01fc,0x20c8bdd6,0xeea14a6b,0x35171d87,0x6d08842d,0x9fe1ec4b,0xe19fa4c8,0xf90ead61,0x20d8323a,0x9062cb3c,0x9afe9917,0x17f3a444,0x4be27922,0xf9a5a452,0xae7b9c12,0x365100d8,0xa13d429c,0x90b64b37,0x34a75b32,0x6f0f1257,0x7902aa5,0x3177c4ed,0x1bbde52,0x69f9afa9,0x90884754,0xcb5837fc,0xfc277f8e,0xcd257a6c,0x83e7d849,0xd96e50bd,0xaf87df5d,0x2abbf3db,0xc4c72a4b,0xcd8b1996,0x99b706f8,0x7ef49ec4,0xb9a130b5,0xd4ddd56,0xa4beb677,0x594cf457,0x8b15a959,0x765ce30a,0x2468149f,0xb8aa0914,0x7176b930,0xefbbc448,0xb7f570a2,0xcc7bc84c,0x352deaa8,0xfa627e25,0xb609e953,0x9fa9824e,0x7173e201,0x938b85c3,0xa9ad3f94,0xe09e8c93,0xfa902e79,0x1511867e};
  uint32_t k = kv_crc_c( cmd, len, 0x2169e7fb );
  uint8_t  c = ht[ k % 1024 ];
  return hashes[ c ] == k ? (RedisCmd) c : NO_CMD;
}

}
}
#endif
