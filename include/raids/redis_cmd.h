#ifndef __rai_raids__redis_cmd_h__
#define __rai_raids__redis_cmd_h__

#include <raids/redis_cmd_db.h>
#include <raikv/key_hash.h>

namespace rai {
namespace ds {

enum RedisCatg {
  NO_CATG                =  0,
  CONNECTION_CATG        =  1,
  GEO_CATG               =  2,
  HASH_CATG              =  3,
  HYPERLOGLOG_CATG       =  4,
  KEY_CATG               =  5,
  LIST_CATG              =  6,
  PUBSUB_CATG            =  7,
  SERVER_CATG            =  8,
  SET_CATG               =  9,
  SORTED_SET_CATG        = 10,
  STRING_CATG            = 11,
  TRANSACTION_CATG       = 12,
  STREAM_CATG            = 13
};

enum RedisCmd {
  NO_CMD                 =   0,
  /* Connection */
  ECHO_CMD               =   1,
  PING_CMD               =   2,
  QUIT_CMD               =   3,
  /* Geo */
  GEOADD_CMD             =   4,
  GEOHASH_CMD            =   5,
  GEOPOS_CMD             =   6,
  GEODIST_CMD            =   7,
  GEORADIUS_CMD          =   8,
  GEORADIUSBYMEMBER_CMD  =   9,
  /* Hash */
  HAPPEND_CMD            =  10,
  HDEL_CMD               =  11,
  HEXISTS_CMD            =  12,
  HGET_CMD               =  13,
  HGETALL_CMD            =  14,
  HINCRBY_CMD            =  15,
  HINCRBYFLOAT_CMD       =  16,
  HKEYS_CMD              =  17,
  HLEN_CMD               =  18,
  HMGET_CMD              =  19,
  HMSET_CMD              =  20,
  HSET_CMD               =  21,
  HSETNX_CMD             =  22,
  HSTRLEN_CMD            =  23,
  HVALS_CMD              =  24,
  HSCAN_CMD              =  25,
  /* Hyperloglog */
  PFADD_CMD              =  26,
  PFCOUNT_CMD            =  27,
  PFMERGE_CMD            =  28,
  /* Key */
  DEL_CMD                =  29,
  DUMP_CMD               =  30,
  EXISTS_CMD             =  31,
  EXPIRE_CMD             =  32,
  EXPIREAT_CMD           =  33,
  KEYS_CMD               =  34,
  OBJECT_CMD             =  35,
  PERSIST_CMD            =  36,
  PEXPIRE_CMD            =  37,
  PEXPIREAT_CMD          =  38,
  PTTL_CMD               =  39,
  RANDOMKEY_CMD          =  40,
  RENAME_CMD             =  41,
  RENAMENX_CMD           =  42,
  TOUCH_CMD              =  43,
  TTL_CMD                =  44,
  TYPE_CMD               =  45,
  UNLINK_CMD             =  46,
  SCAN_CMD               =  47,
  /* List */
  BLPOP_CMD              =  48,
  BRPOP_CMD              =  49,
  BRPOPLPUSH_CMD         =  50,
  LINDEX_CMD             =  51,
  LINSERT_CMD            =  52,
  LLEN_CMD               =  53,
  LPOP_CMD               =  54,
  LPUSH_CMD              =  55,
  LPUSHX_CMD             =  56,
  LRANGE_CMD             =  57,
  LREM_CMD               =  58,
  LSET_CMD               =  59,
  LTRIM_CMD              =  60,
  RPOP_CMD               =  61,
  RPOPLPUSH_CMD          =  62,
  RPUSH_CMD              =  63,
  RPUSHX_CMD             =  64,
  /* Pubsub */
  PSUBSCRIBE_CMD         =  65,
  PUBSUB_CMD             =  66,
  PUBLISH_CMD            =  67,
  PUNSUBSCRIBE_CMD       =  68,
  SUBSCRIBE_CMD          =  69,
  UNSUBSCRIBE_CMD        =  70,
  /* Server */
  CLIENT_CMD             =  71,
  COMMAND_CMD            =  72,
  CONFIG_CMD             =  73,
  DBSIZE_CMD             =  74,
  INFO_CMD               =  75,
  MONITOR_CMD            =  76,
  SHUTDOWN_CMD           =  77,
  TIME_CMD               =  78,
  /* Set */
  SADD_CMD               =  79,
  SCARD_CMD              =  80,
  SDIFF_CMD              =  81,
  SDIFFSTORE_CMD         =  82,
  SINTER_CMD             =  83,
  SINTERSTORE_CMD        =  84,
  SISMEMBER_CMD          =  85,
  SMEMBERS_CMD           =  86,
  SMOVE_CMD              =  87,
  SPOP_CMD               =  88,
  SRANDMEMBER_CMD        =  89,
  SREM_CMD               =  90,
  SUNION_CMD             =  91,
  SUNIONSTORE_CMD        =  92,
  SSCAN_CMD              =  93,
  /* Sorted Set */
  ZADD_CMD               =  94,
  ZCARD_CMD              =  95,
  ZCOUNT_CMD             =  96,
  ZINCRBY_CMD            =  97,
  ZINTERSTORE_CMD        =  98,
  ZLEXCOUNT_CMD          =  99,
  ZRANGE_CMD             = 100,
  ZRANGEBYLEX_CMD        = 101,
  ZREVRANGEBYLEX_CMD     = 102,
  ZRANGEBYSCORE_CMD      = 103,
  ZRANK_CMD              = 104,
  ZREM_CMD               = 105,
  ZREMRANGEBYLEX_CMD     = 106,
  ZREMRANGEBYRANK_CMD    = 107,
  ZREMRANGEBYSCORE_CMD   = 108,
  ZREVRANGE_CMD          = 109,
  ZREVRANGEBYSCORE_CMD   = 110,
  ZREVRANK_CMD           = 111,
  ZSCORE_CMD             = 112,
  ZUNIONSTORE_CMD        = 113,
  ZSCAN_CMD              = 114,
  ZPOPMIN_CMD            = 115,
  ZPOPMAX_CMD            = 116,
  BZPOPMIN_CMD           = 117,
  BZPOPMAX_CMD           = 118,
  /* String */
  APPEND_CMD             = 119,
  BITCOUNT_CMD           = 120,
  BITFIELD_CMD           = 121,
  BITOP_CMD              = 122,
  BITPOS_CMD             = 123,
  DECR_CMD               = 124,
  DECRBY_CMD             = 125,
  GET_CMD                = 126,
  GETBIT_CMD             = 127,
  GETRANGE_CMD           = 128,
  GETSET_CMD             = 129,
  INCR_CMD               = 130,
  INCRBY_CMD             = 131,
  INCRBYFLOAT_CMD        = 132,
  MGET_CMD               = 133,
  MSET_CMD               = 134,
  MSETNX_CMD             = 135,
  PSETEX_CMD             = 136,
  SET_CMD                = 137,
  SETBIT_CMD             = 138,
  SETEX_CMD              = 139,
  SETNX_CMD              = 140,
  SETRANGE_CMD           = 141,
  STRLEN_CMD             = 142,
  /* Transaction */
  DISCARD_CMD            = 143,
  EXEC_CMD               = 144,
  MULTI_CMD              = 145,
  UNWATCH_CMD            = 146,
  WATCH_CMD              = 147,
  /* Stream */
  XINFO_CMD              = 148,
  XADD_CMD               = 149,
  XTRIM_CMD              = 150,
  XDEL_CMD               = 151,
  XRANGE_CMD             = 152,
  XREVRANGE_CMD          = 153,
  XLEN_CMD               = 154,
  XREAD_CMD              = 155,
  XGROUP_CMD             = 156,
  XREADGROUP_CMD         = 157,
  XACK_CMD               = 158,
  XCLAIM_CMD             = 159,
  XPENDING_CMD           = 160,
  XSETID_CMD             = 161
};

static const uint8_t cmd_hash_ht[ 1024 ] = {
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,81,0,0,0,159,0,0,0,0,0,0,0,0,0,27,
0,0,0,0,0,133,0,14,0,0,19,0,0,0,0,0,0,0,0,138,0,0,0,0,0,0,0,131,0,69,0,0,
0,0,0,9,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,91,0,123,0,0,0,0,0,0,140,0,117,0,0,0,0,0,0,0,148,0,0,0,
0,55,0,0,0,0,0,0,0,101,0,0,71,0,82,0,0,0,0,0,112,0,0,0,0,114,0,0,0,0,0,0,
122,0,0,33,93,86,0,0,0,0,0,0,0,0,0,0,0,0,0,150,0,0,0,0,0,0,161,0,0,11,0,0,
0,77,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,153,0,0,0,94,0,12,
0,0,0,0,0,0,0,0,0,0,0,76,0,0,0,0,0,50,98,0,0,0,0,0,0,0,23,0,0,0,0,0,
0,0,0,0,0,0,143,0,0,0,0,0,0,0,64,0,135,0,0,0,0,0,67,0,0,0,0,0,0,0,15,73,
0,0,0,0,0,0,88,44,0,0,0,0,0,87,0,0,0,0,0,0,0,0,0,0,0,0,0,115,0,0,0,0,
70,0,0,52,0,0,0,0,0,0,0,85,0,0,0,0,0,0,0,43,0,0,0,0,45,0,0,29,0,0,0,0,
0,0,0,0,107,0,0,57,0,0,0,0,0,0,32,0,0,0,0,0,0,0,0,0,0,0,40,0,0,0,22,0,
151,0,0,79,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,96,0,0,0,0,1,0,
75,0,0,89,0,0,0,0,0,0,0,0,124,0,0,0,0,0,0,0,0,134,0,0,26,0,20,0,0,0,0,0,
0,0,0,0,0,0,0,42,0,0,0,28,0,0,0,0,58,0,102,0,0,0,113,0,0,0,0,0,0,0,0,142,
65,0,0,66,0,0,0,0,0,0,0,0,74,0,0,0,111,0,0,0,0,141,0,0,0,0,31,0,0,0,0,0,
0,0,0,0,116,0,0,0,0,0,0,0,0,0,0,0,0,0,25,0,0,0,0,0,0,0,39,0,0,47,0,0,
0,0,0,0,0,0,0,0,97,0,0,0,128,0,152,0,0,0,0,0,0,0,160,0,0,0,0,0,0,158,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,147,0,0,0,0,0,132,0,36,0,0,0,83,0,30,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,136,0,
0,46,0,0,0,0,0,156,126,95,0,62,0,0,0,18,120,0,0,0,10,0,0,0,0,0,0,56,0,0,0,105,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,80,0,0,35,109,0,0,0,5,0,0,0,
0,0,0,0,0,0,6,0,0,0,0,0,0,104,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,2,0,0,
0,0,0,0,0,0,0,0,0,0,129,0,0,100,3,0,0,103,0,0,0,0,0,0,0,0,0,24,38,0,21,84,
0,0,0,0,0,0,0,0,0,0,0,0,0,59,0,0,0,145,0,0,0,0,0,0,137,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,99,63,0,0,0,125,0,0,0,0,0,0,68,54,0,0,0,0,146,0,0,0,
78,0,0,0,0,0,0,16,0,0,0,118,0,0,0,0,0,0,0,139,34,0,155,0,49,0,0,17,149,0,0,0,
0,144,8,0,0,0,0,51,0,0,0,0,0,0,13,0,0,0,0,0,0,0,0,0,0,0,127,0,53,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,108,0,0,0,0,61,0,
0,0,0,0,0,0,0,0,0,0,0,0,110,0,0,0,0,119,154,41,0,0,0,130,0,0,0,0,0,0,37,0,
0,90,0,0,0,106,0,0,0,157,0,0,0,0,0,0,60,0,0,0,0,0,0,0,0,0,0,92,0,0,0,0,
72,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,48,0,0,0,121,0,0,0,0,0,0,0,0,0,0};

static inline uint32_t
get_redis_cmd_hash( const void *cmd,  size_t len ) {
  uint32_t out[ MAX_CMD_LEN / 4 ];
  for ( size_t i = 0; i * 4 < len; i++ )
    out[ i ] = ((const uint32_t *) cmd)[ i ] & 0xdfdfdfdfU;
  return kv_crc_c( out, len, 0x36fbdccd );
}

static inline RedisCmd
get_redis_cmd( uint32_t h ) {
  return (RedisCmd) cmd_hash_ht[ h % 1024 ];
}

extern const RedisCmdExtra xtra[ 811 ];

static const size_t REDIS_CMD_DB_SIZE = 162;
static const RedisCmdData
cmd_db[ REDIS_CMD_DB_SIZE ] = {
{ "none",NULL,0,NO_CMD,CMD_NOFLAGS,4,NO_CATG,0,0,0,0 },
{ "echo",&xtra[0],0x9580359e,ECHO_CMD,CMD_NOFLAGS,4,CONNECTION_CATG,2,0,0,0 },
{ "ping",&xtra[5],0x4eab7add,PING_CMD,CMD_NOFLAGS,4,CONNECTION_CATG,-1,0,0,0 },
{ "quit",&xtra[10],0x3ca0caee,QUIT_CMD,CMD_NOFLAGS,4,CONNECTION_CATG,-1,0,0,0 },
{ "geoadd",&xtra[15],0xb9b8cd97,GEOADD_CMD,CMD_WRITE_FLAG,6,GEO_CATG,-5,1,1,1 },
{ "geohash",&xtra[19],0xd84fa2bc,GEOHASH_CMD,CMD_READ_FLAG,7,GEO_CATG,-2,1,1,1 },
{ "geopos",&xtra[23],0x13ceac6,GEOPOS_CMD,CMD_READ_FLAG,6,GEO_CATG,-2,1,1,1 },
{ "geodist",&xtra[27],0x3065c6d3,GEODIST_CMD,CMD_READ_FLAG,7,GEO_CATG,-4,1,1,1 },
{ "georadius",&xtra[31],0xda4fa362,GEORADIUS_CMD,CMD_WRITE_MV_FLAG,9,GEO_CATG,-6,1,1,1 },
{ "georadiusbymember",&xtra[35],0x8b414043,GEORADIUSBYMEMBER_CMD,CMD_WRITE_MV_FLAG,17,GEO_CATG,-5,1,1,1 },
{ "happend",&xtra[39],0xe805ce94,HAPPEND_CMD,CMD_WRITE_FLAG,7,HASH_CATG,-4,1,1,1 },
{ "hdel",&xtra[44],0x623f60bd,HDEL_CMD,CMD_WRITE_FLAG,4,HASH_CATG,-3,1,1,1 },
{ "hexists",&xtra[49],0xbde1a0df,HEXISTS_CMD,CMD_READ_FLAG,7,HASH_CATG,3,1,1,1 },
{ "hget",&xtra[54],0x12963f6e,HGET_CMD,CMD_READ_FLAG,4,HASH_CATG,3,1,1,1 },
{ "hgetall",&xtra[59],0x9e6a1427,HGETALL_CMD,CMD_READ_FLAG,7,HASH_CATG,2,1,1,1 },
{ "hincrby",&xtra[64],0xa1ddd51e,HINCRBY_CMD,CMD_WRITE_FLAG,7,HASH_CATG,4,1,1,1 },
{ "hincrbyfloat",&xtra[69],0xb4db7b47,HINCRBYFLOAT_CMD,CMD_WRITE_FLAG,12,HASH_CATG,4,1,1,1 },
{ "hkeys",&xtra[74],0xc283fb5b,HKEYS_CMD,CMD_READ_FLAG,5,HASH_CATG,2,1,1,1 },
{ "hlen",&xtra[79],0xbb552e8f,HLEN_CMD,CMD_READ_FLAG,4,HASH_CATG,2,1,1,1 },
{ "hmget",&xtra[84],0x71388c2a,HMGET_CMD,CMD_READ_FLAG,5,HASH_CATG,-3,1,1,1 },
{ "hmset",&xtra[89],0x9f4455ba,HMSET_CMD,CMD_WRITE_FLAG,5,HASH_CATG,-4,1,1,1 },
{ "hset",&xtra[94],0xfceae6fe,HSET_CMD,CMD_WRITE_FLAG,4,HASH_CATG,-4,1,1,1 },
{ "hsetnx",&xtra[99],0xa5e0d97e,HSETNX_CMD,CMD_WRITE_FLAG,6,HASH_CATG,4,1,1,1 },
{ "hstrlen",&xtra[104],0xf3a62cfa,HSTRLEN_CMD,CMD_READ_FLAG,7,HASH_CATG,3,1,1,1 },
{ "hvals",&xtra[109],0x87f71efb,HVALS_CMD,CMD_READ_FLAG,5,HASH_CATG,2,1,1,1 },
{ "hscan",&xtra[114],0x1b56612,HSCAN_CMD,CMD_READ_FLAG,5,HASH_CATG,-3,1,1,1 },
{ "pfadd",&xtra[119],0x7d56a5b8,PFADD_CMD,CMD_WRITE_FLAG,5,HYPERLOGLOG_CATG,-2,1,1,1 },
{ "pfcount",&xtra[124],0x1df7341f,PFCOUNT_CMD,CMD_READ_FLAG,7,HYPERLOGLOG_CATG,-2,1,-1,1 },
{ "pfmerge",&xtra[129],0x19a765cb,PFMERGE_CMD,CMD_WRITE_FLAG,7,HYPERLOGLOG_CATG,-2,1,-1,1 },
{ "del",&xtra[134],0x3a589d5b,DEL_CMD,CMD_WRITE_FLAG,3,KEY_CATG,-2,1,-1,1 },
{ "dump",&xtra[139],0xfab3165b,DUMP_CMD,CMD_READ_FLAG,4,KEY_CATG,2,1,1,1 },
{ "exists",&xtra[144],0xf0fcb9fa,EXISTS_CMD,CMD_READ_FLAG,6,KEY_CATG,-2,1,-1,1 },
{ "expire",&xtra[149],0x6a35a16e,EXPIRE_CMD,CMD_WRITE_FLAG,6,KEY_CATG,3,1,1,1 },
{ "expireat",&xtra[154],0x5e6df8a3,EXPIREAT_CMD,CMD_WRITE_FLAG,8,KEY_CATG,3,1,1,1 },
{ "keys",&xtra[159],0x74ad354,KEYS_CMD,CMD_READ_FLAG,4,KEY_CATG,2,0,0,0 },
{ "object",&xtra[164],0x2342eab7,OBJECT_CMD,CMD_READ_FLAG,6,KEY_CATG,-2,2,2,2 },
{ "persist",&xtra[169],0xd3ae3e55,PERSIST_CMD,CMD_WRITE_FLAG,7,KEY_CATG,2,1,1,1 },
{ "pexpire",&xtra[174],0xf55ebfbe,PEXPIRE_CMD,CMD_WRITE_FLAG,7,KEY_CATG,3,1,1,1 },
{ "pexpireat",&xtra[179],0xd95ad2fc,PEXPIREAT_CMD,CMD_WRITE_FLAG,9,KEY_CATG,3,1,1,1 },
{ "pttl",&xtra[184],0x21daa61a,PTTL_CMD,CMD_READ_FLAG,4,KEY_CATG,2,1,1,1 },
{ "randomkey",&xtra[189],0x97057a,RANDOMKEY_CMD,CMD_READ_FLAG,9,KEY_CATG,1,0,0,0 },
{ "rename",&xtra[194],0x85e9d3b3,RENAME_CMD,CMD_WRITE_FLAG,6,KEY_CATG,3,1,2,1 },
{ "renamenx",&xtra[199],0x9fd901c7,RENAMENX_CMD,CMD_WRITE_FLAG,8,KEY_CATG,3,1,2,1 },
{ "touch",&xtra[204],0x6f400d53,TOUCH_CMD,CMD_WRITE_FLAG,5,KEY_CATG,-2,1,1,1 },
{ "ttl",&xtra[209],0x669d8927,TTL_CMD,CMD_READ_FLAG,3,KEY_CATG,2,1,1,1 },
{ "type",&xtra[214],0x6f0e2558,TYPE_CMD,CMD_READ_FLAG,4,KEY_CATG,2,1,1,1 },
{ "unlink",&xtra[219],0x915d9e81,UNLINK_CMD,CMD_WRITE_FLAG,6,KEY_CATG,-2,1,-1,1 },
{ "scan",&xtra[224],0xc47c4e1d,SCAN_CMD,CMD_READ_FLAG,4,KEY_CATG,-2,0,0,0 },
{ "blpop",&xtra[229],0x60a18ff1,BLPOP_CMD,CMD_WRITE_FLAG,5,LIST_CATG,-3,1,-2,1 },
{ "brpop",&xtra[234],0xbbc44f58,BRPOP_CMD,CMD_WRITE_FLAG,5,LIST_CATG,-3,1,-2,1 },
{ "brpoplpush",&xtra[239],0x1a6a74f1,BRPOPLPUSH_CMD,CMD_WRITE_FLAG,10,LIST_CATG,4,1,2,1 },
{ "lindex",&xtra[244],0x6045a767,LINDEX_CMD,CMD_READ_FLAG,6,LIST_CATG,3,1,1,1 },
{ "linsert",&xtra[249],0x69e60543,LINSERT_CMD,CMD_WRITE_FLAG,7,LIST_CATG,5,1,1,1 },
{ "llen",&xtra[254],0xc0771f7c,LLEN_CMD,CMD_READ_FLAG,4,LIST_CATG,2,1,1,1 },
{ "lpop",&xtra[259],0x102d7337,LPOP_CMD,CMD_WRITE_FLAG,4,LIST_CATG,2,1,1,1 },
{ "lpush",&xtra[264],0x1ce0fc81,LPUSH_CMD,CMD_WRITE_FLAG,5,LIST_CATG,-3,1,1,1 },
{ "lpushx",&xtra[269],0xab7dda9b,LPUSHX_CMD,CMD_WRITE_FLAG,6,LIST_CATG,-3,1,1,1 },
{ "lrange",&xtra[274],0x727a7167,LRANGE_CMD,CMD_READ_FLAG,6,LIST_CATG,4,1,1,1 },
{ "lrem",&xtra[279],0x4a6559d0,LREM_CMD,CMD_WRITE_FLAG,4,LIST_CATG,4,1,1,1 },
{ "lset",&xtra[284],0x87c8d70d,LSET_CMD,CMD_WRITE_FLAG,4,LIST_CATG,4,1,1,1 },
{ "ltrim",&xtra[289],0xa357bfd0,LTRIM_CMD,CMD_WRITE_FLAG,5,LIST_CATG,4,1,1,1 },
{ "rpop",&xtra[294],0xcb48b39e,RPOP_CMD,CMD_WRITE_FLAG,4,LIST_CATG,2,1,1,1 },
{ "rpoplpush",&xtra[299],0xb2ef28b,RPOPLPUSH_CMD,CMD_WRITE_FLAG,9,LIST_CATG,3,1,2,1 },
{ "rpush",&xtra[304],0xc6c2f72b,RPUSH_CMD,CMD_WRITE_FLAG,5,LIST_CATG,-3,1,1,1 },
{ "rpushx",&xtra[309],0x620e650e,RPUSHX_CMD,CMD_WRITE_FLAG,6,LIST_CATG,-3,1,1,1 },
{ "psubscribe",&xtra[314],0xb8e631e0,PSUBSCRIBE_CMD,CMD_NOFLAGS,10,PUBSUB_CATG,-2,0,0,0 },
{ "pubsub",&xtra[319],0x15f739e3,PUBSUB_CMD,CMD_NOFLAGS,6,PUBSUB_CATG,-2,0,0,0 },
{ "publish",&xtra[327],0xc7bd0d16,PUBLISH_CMD,CMD_NOFLAGS,7,PUBSUB_CATG,3,0,0,0 },
{ "punsubscribe",&xtra[332],0xb4abbb36,PUNSUBSCRIBE_CMD,CMD_NOFLAGS,12,PUBSUB_CATG,-1,0,0,0 },
{ "subscribe",&xtra[337],0xb757e03d,SUBSCRIBE_CMD,CMD_NOFLAGS,9,PUBSUB_CATG,-2,0,0,0 },
{ "unsubscribe",&xtra[342],0x6e3e2140,UNSUBSCRIBE_CMD,CMD_NOFLAGS,11,PUBSUB_CATG,-1,0,0,0 },
{ "client",&xtra[347],0x6af1c8c,CLIENT_CMD,CMD_ADMIN_FLAG,6,SERVER_CATG,-2,0,0,0 },
{ "command",&xtra[361],0xa2da9fe0,COMMAND_CMD,CMD_NOFLAGS,7,SERVER_CATG,-1,0,0,0 },
{ "config",&xtra[365],0xc4d1291f,CONFIG_CMD,CMD_ADMIN_FLAG,6,SERVER_CATG,-2,0,0,0 },
{ "dbsize",&xtra[368],0x405435ec,DBSIZE_CMD,CMD_NOFLAGS,6,SERVER_CATG,1,0,0,0 },
{ "info",&xtra[372],0xeff3fda0,INFO_CMD,CMD_NOFLAGS,4,SERVER_CATG,-1,0,0,0 },
{ "monitor",&xtra[376],0xc4608ceb,MONITOR_CMD,CMD_ADMIN_FLAG,7,SERVER_CATG,1,0,0,0 },
{ "shutdown",&xtra[381],0x4d5814c1,SHUTDOWN_CMD,CMD_ADMIN_FLAG,8,SERVER_CATG,-1,0,0,0 },
{ "time",&xtra[384],0xe0559340,TIME_CMD,CMD_NOFLAGS,4,SERVER_CATG,1,0,0,0 },
{ "sadd",&xtra[389],0xbdd9cd83,SADD_CMD,CMD_WRITE_FLAG,4,SET_CATG,-3,1,1,1 },
{ "scard",&xtra[394],0xc5082eb4,SCARD_CMD,CMD_READ_FLAG,5,SET_CATG,2,1,1,1 },
{ "sdiff",&xtra[399],0x742d4811,SDIFF_CMD,CMD_READ_FLAG,5,SET_CATG,-2,1,-1,1 },
{ "sdiffstore",&xtra[404],0x28f1c48e,SDIFFSTORE_CMD,CMD_WRITE_FLAG,10,SET_CATG,-3,1,-1,1 },
{ "sinter",&xtra[409],0x5d462659,SINTER_CMD,CMD_READ_FLAG,6,SET_CATG,-2,1,-1,1 },
{ "sinterstore",&xtra[414],0x9126a2ff,SINTERSTORE_CMD,CMD_WRITE_FLAG,11,SET_CATG,-3,1,-1,1 },
{ "sismember",&xtra[419],0x97ed994b,SISMEMBER_CMD,CMD_READ_FLAG,9,SET_CATG,3,1,1,1 },
{ "smembers",&xtra[424],0x73c6c4a5,SMEMBERS_CMD,CMD_READ_FLAG,8,SET_CATG,2,1,1,1 },
{ "smove",&xtra[429],0xa208752d,SMOVE_CMD,CMD_WRITE_FLAG,5,SET_CATG,4,1,2,1 },
{ "spop",&xtra[434],0x160d1926,SPOP_CMD,CMD_WRITE_FLAG,4,SET_CATG,-2,1,1,1 },
{ "srandmember",&xtra[439],0x38ebbda3,SRANDMEMBER_CMD,CMD_READ_FLAG,11,SET_CATG,-2,1,1,1 },
{ "srem",&xtra[444],0x4c4533c1,SREM_CMD,CMD_WRITE_FLAG,4,SET_CATG,-3,1,1,1 },
{ "sunion",&xtra[449],0x63667c69,SUNION_CMD,CMD_READ_FLAG,6,SET_CATG,-2,1,-1,1 },
{ "sunionstore",&xtra[454],0x5b9ca3db,SUNIONSTORE_CMD,CMD_WRITE_FLAG,11,SET_CATG,-3,1,-1,1 },
{ "sscan",&xtra[459],0x3c3bca4,SSCAN_CMD,CMD_READ_FLAG,5,SET_CATG,-3,1,1,1 },
{ "zadd",&xtra[464],0x96d804dd,ZADD_CMD,CMD_WRITE_FLAG,4,SORTED_SET_CATG,-4,1,1,1 },
{ "zcard",&xtra[469],0x387e4a89,ZCARD_CMD,CMD_READ_FLAG,5,SORTED_SET_CATG,2,1,1,1 },
{ "zcount",&xtra[474],0x90633599,ZCOUNT_CMD,CMD_READ_FLAG,6,SORTED_SET_CATG,4,1,1,1 },
{ "zincrby",&xtra[479],0xed807628,ZINCRBY_CMD,CMD_WRITE_FLAG,7,SORTED_SET_CATG,4,1,1,1 },
{ "zinterstore",&xtra[484],0xaa6930f2,ZINTERSTORE_CMD,CMD_WRITE_MV_FLAG,11,SORTED_SET_CATG,-4,1,1,1 },
{ "zlexcount",&xtra[489],0x63f59f2a,ZLEXCOUNT_CMD,CMD_READ_FLAG,9,SORTED_SET_CATG,4,1,1,1 },
{ "zrange",&xtra[494],0xdb0fd6ed,ZRANGE_CMD,CMD_READ_FLAG,6,SORTED_SET_CATG,-4,1,1,1 },
{ "zrangebylex",&xtra[499],0x63952c89,ZRANGEBYLEX_CMD,CMD_READ_FLAG,11,SORTED_SET_CATG,-4,1,1,1 },
{ "zrevrangebylex",&xtra[504],0xb4c98dd2,ZREVRANGEBYLEX_CMD,CMD_READ_FLAG,14,SORTED_SET_CATG,-4,1,1,1 },
{ "zrangebyscore",&xtra[509],0xa9b2e2f1,ZRANGEBYSCORE_CMD,CMD_READ_FLAG,13,SORTED_SET_CATG,-4,1,1,1 },
{ "zrank",&xtra[514],0xbe173ecd,ZRANK_CMD,CMD_READ_FLAG,5,SORTED_SET_CATG,3,1,1,1 },
{ "zrem",&xtra[519],0x6744fa9f,ZREM_CMD,CMD_WRITE_FLAG,4,SORTED_SET_CATG,-3,1,1,1 },
{ "zremrangebylex",&xtra[524],0xf9193bc5,ZREMRANGEBYLEX_CMD,CMD_WRITE_FLAG,14,SORTED_SET_CATG,4,1,1,1 },
{ "zremrangebyrank",&xtra[529],0x3dc65964,ZREMRANGEBYRANK_CMD,CMD_WRITE_FLAG,15,SORTED_SET_CATG,4,1,1,1 },
{ "zremrangebyscore",&xtra[534],0x41afbf99,ZREMRANGEBYSCORE_CMD,CMD_WRITE_FLAG,16,SORTED_SET_CATG,4,1,1,1 },
{ "zrevrange",&xtra[539],0x54ce86b8,ZREVRANGE_CMD,CMD_READ_FLAG,9,SORTED_SET_CATG,-4,1,1,1 },
{ "zrevrangebyscore",&xtra[544],0x90b35fac,ZREVRANGEBYSCORE_CMD,CMD_READ_FLAG,16,SORTED_SET_CATG,-4,1,1,1 },
{ "zrevrank",&xtra[549],0xb44209f0,ZREVRANK_CMD,CMD_READ_FLAG,8,SORTED_SET_CATG,3,1,1,1 },
{ "zscore",&xtra[554],0x9bd50094,ZSCORE_CMD,CMD_READ_FLAG,6,SORTED_SET_CATG,3,1,1,1 },
{ "zunionstore",&xtra[559],0x60d331d6,ZUNIONSTORE_CMD,CMD_WRITE_MV_FLAG,11,SORTED_SET_CATG,-4,1,1,1 },
{ "zscan",&xtra[564],0xfeb5d899,ZSCAN_CMD,CMD_READ_FLAG,5,SORTED_SET_CATG,-3,1,1,1 },
{ "zpopmin",&xtra[569],0x40898d3b,ZPOPMIN_CMD,CMD_WRITE_FLAG,7,SORTED_SET_CATG,-2,1,1,1 },
{ "zpopmax",&xtra[574],0xeb626e04,ZPOPMAX_CMD,CMD_WRITE_FLAG,7,SORTED_SET_CATG,-2,1,1,1 },
{ "bzpopmin",&xtra[579],0x85cbf474,BZPOPMIN_CMD,CMD_WRITE_FLAG,8,SORTED_SET_CATG,-3,1,-2,1 },
{ "bzpopmax",&xtra[584],0x2e20174b,BZPOPMAX_CMD,CMD_WRITE_FLAG,8,SORTED_SET_CATG,-3,1,-2,1 },
{ "append",&xtra[589],0xa518d7b1,APPEND_CMD,CMD_WRITE_FLAG,6,STRING_CATG,3,1,1,1 },
{ "bitcount",&xtra[594],0x7940f690,BITCOUNT_CMD,CMD_READ_FLAG,8,STRING_CATG,-2,1,1,1 },
{ "bitfield",&xtra[599],0x22e8bff5,BITFIELD_CMD,CMD_WRITE_FLAG,8,STRING_CATG,-2,1,1,1 },
{ "bitop",&xtra[610],0x5818b0a0,BITOP_CMD,CMD_WRITE_FLAG,5,STRING_CATG,-4,2,-1,1 },
{ "bitpos",&xtra[615],0x4d9586b,BITPOS_CMD,CMD_READ_FLAG,6,STRING_CATG,-3,1,1,1 },
{ "decr",&xtra[620],0x9ff189ac,DECR_CMD,CMD_WRITE_FLAG,4,STRING_CATG,2,1,1,1 },
{ "decrby",&xtra[625],0x5c57332f,DECRBY_CMD,CMD_WRITE_FLAG,6,STRING_CATG,3,1,1,1 },
{ "get",&xtra[630],0x4af1c288,GET_CMD,CMD_READ_FLAG,3,STRING_CATG,2,1,1,1 },
{ "getbit",&xtra[635],0xfef6ab7a,GETBIT_CMD,CMD_READ_FLAG,6,STRING_CATG,3,1,1,1 },
{ "getrange",&xtra[640],0xb1c0d22c,GETRANGE_CMD,CMD_READ_FLAG,8,STRING_CATG,4,1,1,1 },
{ "getset",&xtra[645],0xf88be6ea,GETSET_CMD,CMD_WRITE_FLAG,6,STRING_CATG,3,1,1,1 },
{ "incr",&xtra[650],0x1dad8fb7,INCR_CMD,CMD_WRITE_FLAG,4,STRING_CATG,2,1,1,1 },
{ "incrby",&xtra[655],0xecc0cc3b,INCRBY_CMD,CMD_WRITE_FLAG,6,STRING_CATG,3,1,1,1 },
{ "incrbyfloat",&xtra[660],0xf905ee53,INCRBYFLOAT_CMD,CMD_WRITE_FLAG,11,STRING_CATG,3,1,1,1 },
{ "mget",&xtra[665],0xb4f1a425,MGET_CMD,CMD_READ_FLAG,4,STRING_CATG,-2,1,-1,1 },
{ "mset",&xtra[670],0x5a8d7db5,MSET_CMD,CMD_WRITE_FLAG,4,STRING_CATG,-3,1,-1,2 },
{ "msetnx",&xtra[675],0xf8258510,MSETNX_CMD,CMD_WRITE_FLAG,6,STRING_CATG,-3,1,-1,2 },
{ "psetex",&xtra[680],0xac199a7e,PSETEX_CMD,CMD_WRITE_FLAG,6,STRING_CATG,4,1,1,1 },
{ "set",&xtra[685],0xa48d1b18,SET_CMD,CMD_WRITE_FLAG,3,STRING_CATG,-3,1,1,1 },
{ "setbit",&xtra[690],0x8c0fac33,SETBIT_CMD,CMD_WRITE_FLAG,6,STRING_CATG,4,1,1,1 },
{ "setex",&xtra[695],0x52c54753,SETEX_CMD,CMD_WRITE_FLAG,5,STRING_CATG,4,1,1,1 },
{ "setnx",&xtra[700],0xfb362c72,SETNX_CMD,CMD_WRITE_FLAG,5,STRING_CATG,3,1,1,1 },
{ "setrange",&xtra[705],0x14ab59f5,SETRANGE_CMD,CMD_WRITE_FLAG,8,STRING_CATG,4,1,1,1 },
{ "strlen",&xtra[710],0xbebb35df,STRLEN_CMD,CMD_READ_FLAG,6,STRING_CATG,2,1,1,1 },
{ "discard",&xtra[715],0xd23e9106,DISCARD_CMD,CMD_NOFLAGS,7,TRANSACTION_CATG,1,0,0,0 },
{ "exec",&xtra[718],0xba224361,EXEC_CMD,CMD_NOFLAGS,4,TRANSACTION_CATG,1,0,0,0 },
{ "multi",&xtra[721],0xe7229311,MULTI_CMD,CMD_NOFLAGS,5,TRANSACTION_CATG,1,0,0,0 },
{ "unwatch",&xtra[724],0xd514cb3c,UNWATCH_CMD,CMD_NOFLAGS,7,TRANSACTION_CATG,1,0,0,0 },
{ "watch",&xtra[727],0xb0335e4d,WATCH_CMD,CMD_READ_FLAG,5,TRANSACTION_CATG,-2,1,-1,1 },
{ "xinfo",&xtra[730],0xa518b47c,XINFO_CMD,CMD_READ_FLAG,5,STREAM_CATG,-2,2,2,1 },
{ "xadd",&xtra[740],0x29bf275c,XADD_CMD,CMD_WRITE_FLAG,4,STREAM_CATG,-5,1,1,1 },
{ "xtrim",&xtra[745],0xcc3060b3,XTRIM_CMD,CMD_WRITE_FLAG,5,STREAM_CATG,-2,1,1,1 },
{ "xdel",&xtra[750],0x8b5bd180,XDEL_CMD,CMD_WRITE_FLAG,4,STREAM_CATG,-2,1,1,1 },
{ "xrange",&xtra[755],0x83762e,XRANGE_CMD,CMD_READ_FLAG,6,STREAM_CATG,-4,1,1,1 },
{ "xrevrange",&xtra[760],0xb95f5cd9,XREVRANGE_CMD,CMD_READ_FLAG,9,STREAM_CATG,-4,1,1,1 },
{ "xlen",&xtra[765],0x52319fb2,XLEN_CMD,CMD_READ_FLAG,4,STREAM_CATG,2,1,1,1 },
{ "xread",&xtra[770],0xe9817356,XREAD_CMD,CMD_READ_MV_FLAG,5,STREAM_CATG,-3,1,1,1 },
{ "xgroup",&xtra[775],0x44dd7e87,XGROUP_CMD,CMD_WRITE_FLAG,6,STREAM_CATG,-2,2,2,1 },
{ "xreadgroup",&xtra[786],0x21c71fc9,XREADGROUP_CMD,CMD_WRITE_MV_FLAG,10,STREAM_CATG,-3,1,1,1 },
{ "xack",&xtra[791],0xdc1d23d,XACK_CMD,CMD_WRITE_FLAG,4,STREAM_CATG,-4,1,1,1 },
{ "xclaim",&xtra[796],0xd5301015,XCLAIM_CMD,CMD_WRITE_FLAG,6,STREAM_CATG,-5,1,1,1 },
{ "xpending",&xtra[801],0xe44a9236,XPENDING_CMD,CMD_READ_FLAG,8,STREAM_CATG,-3,1,1,1 },
{ "xsetid",&xtra[806],0x429c28ba,XSETID_CMD,CMD_WRITE_FLAG,6,STREAM_CATG,4,1,1,1 }
};

#ifdef REDIS_XTRA
const RedisCmdExtra xtra[ 811 ] = {
{ &xtra[1], XTRA_SHORT, "echo string"},
{ &xtra[2], XTRA_USAGE, "echo string\n"},
{ &xtra[3], XTRA_EXAMPLE, "> echo hello\n"
"\"hello\"\n"},
{ &xtra[4], XTRA_DESCR, "Echo the string.\n"},
{ NULL, XTRA_RETURN, "The string is sent back.\n"},
{ &xtra[6], XTRA_SHORT, "reply with pong"},
{ &xtra[7], XTRA_USAGE, "ping [string]\n"},
{ &xtra[8], XTRA_EXAMPLE, "> ping\n"
"'PONG'\n"
"> ping hello\n"
"\"hello\"\n"},
{ &xtra[9], XTRA_DESCR,
"Ping sends PONG when used without a string.  Reply with the string\n"
"when it is present.\n"},
{ NULL, XTRA_RETURN, "Either PONG or the string.\n"},
{ &xtra[11], XTRA_SHORT, "close connection"},
{ &xtra[12], XTRA_USAGE, "quit\n"},
{ &xtra[13], XTRA_EXAMPLE, "> quit\n"
"'OK'\n"
"(connection closed)\n"},
{ &xtra[14], XTRA_DESCR, "Close the connection.\n"},
{ NULL, XTRA_RETURN,
"An *OK* is returned, then the connection is closed.\n"},
{ &xtra[16], XTRA_SHORT, "add a pos to key"},
{ &xtra[17], XTRA_USAGE, "geoadd key long lat mem [long lat mem ...]\n"},
{ &xtra[18], XTRA_DESCR,
"Add longitude, latitude to key as member by converting the position to\n"
"a *Uber H3* hash and adding it to the set.  The members are ordered by\n"
"hash because hashes closest to eachother are also closest by spatual\n"
"distance.\n"},
{ NULL, XTRA_RETURN,
"An integer is returned indicating how many members were added which\n"
"did not already exist.\n"},
{ &xtra[20], XTRA_SHORT, "get hash of members"},
{ &xtra[21], XTRA_USAGE, "geohash key mem [mem ...]\n"},
{ &xtra[22], XTRA_DESCR,
"Get geo hash string by finding the members and converting the *Uber\n"
"H3* hash to an encoded hash string as described in on the geohash wiki\n"
"page: https://en.wikipedia.org/wiki/Geohash\n"},
{ NULL, XTRA_RETURN,
"An array of geohash strings, which may be *nil* if member is not\n"
"found.\n"},
{ &xtra[24], XTRA_SHORT, "get pos of members"},
{ &xtra[25], XTRA_USAGE, "geopos key [mem ...]\n"},
{ &xtra[26], XTRA_DESCR,
"Get geo positions of members as longitude, latitude pairs.\n"},
{ NULL, XTRA_RETURN,
"An array of positions or *null* if member is not found.\n"},
{ &xtra[28], XTRA_SHORT, "get member distance"},
{ &xtra[29], XTRA_USAGE, "geodist key mem1 mem2 [unit]\n"},
{ &xtra[30], XTRA_DESCR,
"Compute the distance in units between members.  Default unit is\n"
"meters, but km, feet, miles are accepted.\n"},
{ NULL, XTRA_RETURN,
"A decimal string representing the distance or *nil* when member is not\n"
"found.\n"},
{ &xtra[32], XTRA_SHORT, "get members in radius"},
{ &xtra[33], XTRA_USAGE,
"georadius key long lat rad m | k | ft | mi [withcoord] [withdist]\n"
" [withhash] [count n] [asc | desc] [storedist key]\n"},
{ &xtra[34], XTRA_DESCR,
"Get all members within a radius of position.  This iterates through\n"
"the set using the hash of longitude latitude as the starting point.\n"
"When the endpoints are found, the members are returned or stored.\n"
"When storedist is used, then the distance and members are stored in a\n"
"sorted set.\n"},
{ NULL, XTRA_RETURN,
"An array of members with optional distance, hash, and/or coordinates.\n"
"The hash is an integer used by the *Uber H3* library.\n"},
{ &xtra[36], XTRA_SHORT, "get members"},
{ &xtra[37], XTRA_USAGE,
"georadiusbymember key member rad m | k | ft | mi [withcoord]\n"
" [withdist] [withhash] [count n] [asc | desc] [storedist key]\n"},
{ &xtra[38], XTRA_DESCR,
"Similar to georadius, get all members within a radius of an existing\n"
"member.  This iterates through the set using the named member as the\n"
"starting point.  When the endpoints are found, the members are\n"
"returned or stored.  When storedist is used, then the distance and\n"
"members are stored in a sorted set.\n"},
{ NULL, XTRA_RETURN,
"An array of members with optional distance, hash, and/or coordinates.\n"
"The hash is an integer used by the *Uber H3* library.\n"},
{ &xtra[40], XTRA_SHORT, "append a value"},
{ &xtra[41], XTRA_USAGE, "happend key field val [val ...]\n"},
{ &xtra[42], XTRA_EXAMPLE,
"> happend h f v\n"
"1\n"
"> happend h f w\n"
"0\n"
"> hgetall h\n"
"[\"f\",\"vw\"]\n"},
{ &xtra[43], XTRA_DESCR, "Append a string to a field value.\n"},
{ NULL, XTRA_RETURN,
"An integer 1 or 0 indicating whether the field was created.\n"},
{ &xtra[45], XTRA_SHORT, "delete fields"},
{ &xtra[46], XTRA_USAGE, "hdel key field [field ...]\n"},
{ &xtra[47], XTRA_EXAMPLE, "> hmset h f 1 g 2\n"
"'OK'\n"
"> hdel h f g\n"
"2\n"},
{ &xtra[48], XTRA_DESCR, "Remove one or more fields from a hash set.\n"},
{ NULL, XTRA_RETURN,
"An integer indicating how many fields were removed.\n"},
{ &xtra[50], XTRA_SHORT, "if field exists"},
{ &xtra[51], XTRA_USAGE, "hexists key field\n"},
{ &xtra[52], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hexists h f\n"
"1\n"
"> hexists h h\n"
"0\n"},
{ &xtra[53], XTRA_DESCR, "Test whether field exists in hash.\n"},
{ NULL, XTRA_RETURN,
"An integer *1* indicating the field exists or *0* when it doesn't.\n"},
{ &xtra[55], XTRA_SHORT, "get a field value"},
{ &xtra[56], XTRA_USAGE, "hget key field\n"},
{ &xtra[57], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hget h f\n"
"\"1\"\n"
"> hget h h\n"
"nil\n"},
{ &xtra[58], XTRA_DESCR, "Get the value associated with field.\n"},
{ NULL, XTRA_RETURN,
"A string value or *nil* when the field does not exist.\n"},
{ &xtra[60], XTRA_SHORT, "get all field values"},
{ &xtra[61], XTRA_USAGE, "hgetall key\n"},
{ &xtra[62], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hgetall h\n"
"[\"f\",\"1\",\"g\",\"2\"]\n"},
{ &xtra[63], XTRA_DESCR,
"Get all of the fields and values associated with the hash stored at\n"
"key.\n"},
{ NULL, XTRA_RETURN,
"An array of field value pairs.  An empty array when key doesn't exist.\n"},
{ &xtra[65], XTRA_SHORT, "incr field"},
{ &xtra[66], XTRA_USAGE, "hincrby key field int\n"},
{ &xtra[67], XTRA_EXAMPLE, "> hmset h f 1 g 2\n"
"'OK'\n"
"> hincrby h f 10\n"
"11\n"},
{ &xtra[68], XTRA_DESCR, "Add an integer value to field.\n"},
{ NULL, XTRA_RETURN,
"The integer value after incrementing the integer or an error if the\n"
"value stored in field is not an integer.\n"},
{ &xtra[70], XTRA_SHORT, "incr field"},
{ &xtra[71], XTRA_USAGE, "hincrbyfloat key field num\n"},
{ &xtra[72], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hincrbyfloat h f 123456.66\n"
"\"123457.66\"\n"},
{ &xtra[73], XTRA_DESCR,
"Add a numeric value to field.  This uses 128 bit decimal arithmetic\n"
"which has a 34 digit range and an exponent from -6143 to +6144.\n"
"https://en.wikipedia.org/wiki/Decimal128_floating-point_format\n"},
{ NULL, XTRA_RETURN,
"A string numeric value after incrementing the number or an error if\n"
"the value stored in field is not a number.\n"},
{ &xtra[75], XTRA_SHORT, "get all field keys"},
{ &xtra[76], XTRA_USAGE, "hkeys key\n"},
{ &xtra[77], XTRA_EXAMPLE, "> hmset h f 1 g 2\n"
"'OK'\n"
"> hkeys h\n"
"[\"f\",\"g\"]\n"},
{ &xtra[78], XTRA_DESCR, "Get all field keys in the hash.\n"},
{ NULL, XTRA_RETURN, "An array of field names.\n"},
{ &xtra[80], XTRA_SHORT, "get number of fields"},
{ &xtra[81], XTRA_USAGE, "hlen key\n"},
{ &xtra[82], XTRA_EXAMPLE, "> hmset h f 1 g 2\n"
"'OK'\n"
"> hlen h\n"
"2\n"},
{ &xtra[83], XTRA_DESCR,
"Get the number field value pairs in the hash.\n"},
{ NULL, XTRA_RETURN, "An integer count of the number of fields.\n"},
{ &xtra[85], XTRA_SHORT, "get multiple field vals"},
{ &xtra[86], XTRA_USAGE, "hmget key field [field ...]\n"},
{ &xtra[87], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hmget h f g h\n"
"[\"1\",\"2\",nil]\n"},
{ &xtra[88], XTRA_DESCR,
"Get multiple values from a hash.  If a field doesn't exist, a *nil* is\n"
"returned.\n"},
{ NULL, XTRA_RETURN, "An array of values or *nil*.\n"},
{ &xtra[90], XTRA_SHORT, "set multiple field vals"},
{ &xtra[91], XTRA_USAGE, "hmset key field value [field value ...]\n"},
{ &xtra[92], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hmset h g 3 tree 50\n"
"'OK'\n"
"> hgetall h\n"
"[\"f\",\"1\",\"g\",\"3\",\"tree\",\"50\"]\n"},
{ &xtra[93], XTRA_DESCR, "Set multiple field value pairs in the hash.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[95], XTRA_SHORT, "set a field value"},
{ &xtra[96], XTRA_USAGE, "hset key value [field value ...]\n"},
{ &xtra[97], XTRA_EXAMPLE,
"> hset h f 1 g 2\n"
"2\n"
"> hset h g 3 tree 50\n"
"1\n"
"> hgetall h\n"
"[\"f\",\"1\",\"g\",\"3\",\"tree\",\"50\"]\n"},
{ &xtra[98], XTRA_DESCR,
"Set multiple field value pairs in the hash.  Preexisting fields will\n"
"be overwritten, and new fields will be added to the hash.\n"},
{ NULL, XTRA_RETURN,
"An integer indicating number of fields created.\n"},
{ &xtra[100], XTRA_SHORT, "set if not present"},
{ &xtra[101], XTRA_USAGE, "hsetnx key field value\n"},
{ &xtra[102], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hsetnx h tree 50\n"
"1\n"
"> hsetnx h g 3\n"
"0\n"
"> hgetall h\n"
"[\"f\",\"1\",\"g\",\"2\",\"tree\",\"50\"]\n"},
{ &xtra[103], XTRA_DESCR,
"Set a field in a hash only when it does not exist.\n"},
{ NULL, XTRA_RETURN, "An integer *1* or *0* indicating success.\n"},
{ &xtra[105], XTRA_SHORT, "get strlen of field val"},
{ &xtra[106], XTRA_USAGE, "hstrlen key field\n"},
{ &xtra[107], XTRA_EXAMPLE, "> hsetnx h tree 50\n"
"1\n"
"> hstrlen h tree\n"
"2\n"},
{ &xtra[108], XTRA_DESCR,
"Get the string length of value stored with field, *0* if field does\n"
"not exist.\n"},
{ NULL, XTRA_RETURN,
"An integer string length, which is the count of the 8 bit characters\n"
"in the value.\n"},
{ &xtra[110], XTRA_SHORT, "get all values"},
{ &xtra[111], XTRA_USAGE, "hvals key\n"},
{ &xtra[112], XTRA_EXAMPLE,
"> hmset h f 1 g 2\n"
"'OK'\n"
"> hvals h\n"
"[\"f\",\"1\",\"g\",\"2\"]\n"},
{ &xtra[113], XTRA_DESCR, "Get all of the values stored in the hash.\n"},
{ NULL, XTRA_RETURN,
"An array of all the values or *nil* when hash does not exist.\n"},
{ &xtra[115], XTRA_SHORT, "iterate fields"},
{ &xtra[116], XTRA_USAGE, "hscan key cursor [match pattern] [count int]\n"},
{ &xtra[117], XTRA_EXAMPLE,
"> hset h abc 1 abb 2 abd 3 xyz 4 zzz 5\n"
"5\n"
"> hscan h 0 match a* count 1\n"
"[\"2\",[\"abc\",\"1\"]]\n"
"> hscan h 2 match a* count 1\n"
"[\"3\",[\"abb\",\"2\"]]\n"
"> hscan h 3 match a* count 1\n"
"[\"4\",[\"abd\",\"3\"]]\n"
"> hscan h 4 match a* count 1\n"
"[\"0\",[]]\n"
"> hscan h 0\n"
"[\"0\",[\"abc\",\"1\",\"abb\",\"2\",\"abd\",\"3\",\"xyz\",\"4\",\"zzz\",\"5\"]]\n"
"> hscan h 5 match a* count 1\n"
"[\"0\",[]]\n"},
{ &xtra[118], XTRA_DESCR,
"Get the fields and values which match a pattern.  The cursor is the\n"
"offset into the scan where the results will start.  If the cursor is\n"
"equal to 3, then the match will start at the 3rd field in the hash.\n"},
{ NULL, XTRA_RETURN,
"An array within an array.  The outer array is the cursor counter, the\n"
"inner array is the fields and values.\n"},
{ &xtra[120], XTRA_SHORT, "add elems to hlog"},
{ &xtra[121], XTRA_USAGE, "pfadd key elem [elem ...]\n"},
{ &xtra[122], XTRA_EXAMPLE, "> pfadd hl 1 2 3 4 5 6\n"
"1\n"},
{ &xtra[123], XTRA_DESCR, "Add elems to hyperloglog table.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* is returned when new elements are added, *0* is\n"
"returned when all elements are collisions or duplicates.\n"},
{ &xtra[125], XTRA_SHORT, "get hlog cardinality"},
{ &xtra[126], XTRA_USAGE, "pfcount key [key ...]\n"},
{ &xtra[127], XTRA_EXAMPLE, "> pfadd hl 1 2 3 4 5 6\n"
"1\n"
"> pfcount hl\n"
"6\n"},
{ &xtra[128], XTRA_DESCR,
"Get approximate cardinality of hyperloglog table.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the cardinality estimate.\n"},
{ &xtra[130], XTRA_SHORT, "merge multiple hlogs"},
{ &xtra[131], XTRA_USAGE, "pfmerge dkey skey [skey ...]\n"},
{ &xtra[132], XTRA_EXAMPLE,
"> pfadd hl 1 2 3 4 5 6 10\n"
"1\n"
"> pfcount hl\n"
"7\n"
"> pfadd hl2 1 2 3 4 5 6 7\n"
"1\n"
"> pfcount hl2\n"
"7\n"
"> pfmerge hl2 hl hl2\n"
"'OK'\n"
"> pfcount hl2\n"
"8\n"},
{ &xtra[133], XTRA_DESCR,
"Merge multiple hyperloglog tables and write them to the destination.\n"
"The dest key is not merged, it written to.  A dest key can be used as\n"
"a source key, but the original data in the source will be overwritten\n"
"with the merged data.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[135], XTRA_SHORT, "delete keys"},
{ &xtra[136], XTRA_USAGE, "del key [key ...]\n"},
{ &xtra[137], XTRA_EXAMPLE, "> del h hl hl2 hl3\n"
"4\n"},
{ &xtra[138], XTRA_DESCR, "Delete one or more keys.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of keys deleted is returned.\n"},
{ &xtra[140], XTRA_SHORT, "dump key"},
{ &xtra[141], XTRA_USAGE, "dump key\n"},
{ &xtra[142], XTRA_EXAMPLE,
"> set k sophisticateduniverse\n"
"'OK'\n"
"> dump k\n"
"\"n\\u0210/<\\u0214\\u0186\\u0234JH\\u0252H\\u0231\\u0134U\\u0138\\u0197\"\n"
"\"\\u0000\\u0000\\u0000\\u0002\\u0128\\u0003\\u0002\\u0000k\\u0000\\u0000\"\n"
"\"\\u0000\\u0000\\u0000\\u0000\\u0000sophisticateduniverseo\\u0238\"\n"
"\"\\u0021\\u0021\\u0128\\u0214\\u0186n\\u0210/<\"\n"},
{ &xtra[143], XTRA_DESCR, "Serialize value at key.\n"},
{ NULL, XTRA_RETURN,
"The binary value of the key slot as a bulk string or *nil* when key\n"
"does not exist.\n"},
{ &xtra[145], XTRA_SHORT, "test if key exists"},
{ &xtra[146], XTRA_USAGE, "exists key [key ...]\n"},
{ &xtra[147], XTRA_EXAMPLE, "> exists k l m\n"
"3\n"},
{ &xtra[148], XTRA_DESCR, "Test if one or more keys exists.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of keys that exist.\n"},
{ &xtra[150], XTRA_SHORT, "set key expire secs"},
{ &xtra[151], XTRA_USAGE, "expire key secs\n"},
{ &xtra[152], XTRA_EXAMPLE, "> expire k 10\n"
"1\n"},
{ &xtra[153], XTRA_DESCR, "Set expire seconds.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if key expire time was set, *0* if not.\n"},
{ &xtra[155], XTRA_SHORT, "set key expire utc"},
{ &xtra[156], XTRA_USAGE, "expireat key stamp\n"},
{ &xtra[157], XTRA_EXAMPLE, "> expireat k 1580076128\n"
"1\n"
"> ttl k\n"
"67\n"},
{ &xtra[158], XTRA_DESCR, "Set expire at time.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if key expire time was set, *0* if not.\n"},
{ &xtra[160], XTRA_SHORT, "get keys matching pattern"},
{ &xtra[161], XTRA_USAGE, "keys pattern\n"},
{ &xtra[162], XTRA_EXAMPLE, "> keys k*\n"
"[\"k\",\"kkk\",\"kk\"]\n"},
{ &xtra[163], XTRA_DESCR, "Find all keys matching pattern.\n"},
{ NULL, XTRA_RETURN,
"An array of key strings that match the pattern.\n"},
{ &xtra[165], XTRA_SHORT, "inspect key"},
{ &xtra[166], XTRA_USAGE,
"object [refcount key | encoding key | idletime key | freq key | help]\n"},
{ &xtra[167], XTRA_EXAMPLE,
"> object freq k\n"
"1\n"
"> set k value2\n"
"'OK'\n"
"> object freq k\n"
"2\n"},
{ &xtra[168], XTRA_DESCR, "Inspect key value attributes.\n"},
{ NULL, XTRA_RETURN,
"- refcount, always *1*.\n"
"- encoding, the type of data.\n"
"- idletime, always *0*.\n"
"- freq, the number of time key was updated.\n"},
{ &xtra[170], XTRA_SHORT, "clear expire stamp"},
{ &xtra[171], XTRA_USAGE, "persist key\n"},
{ &xtra[172], XTRA_EXAMPLE, "> persist k\n"
"1\n"},
{ &xtra[173], XTRA_DESCR, "Remove expiration time.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if key exists was cleared, *0* if not.\n"},
{ &xtra[175], XTRA_SHORT, "set expire millisecs"},
{ &xtra[176], XTRA_USAGE, "pexpire key ms\n"},
{ &xtra[177], XTRA_EXAMPLE, "> pexpire k 10000\n"
"1\n"
"> ttl k\n"
"7\n"
"> pttl k\n"
"3986\n"},
{ &xtra[178], XTRA_DESCR, "Set expire ttl in milliseconds.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if key expire time was set, *0* if not.\n"},
{ &xtra[180], XTRA_SHORT, "set expire ms utc"},
{ &xtra[181], XTRA_USAGE, "pexpireat key ms\n"},
{ &xtra[182], XTRA_EXAMPLE,
"> pexpireat k 1580077977700\n"
"1\n"
"> ttl k\n"
"59\n"
"> pttl k\n"
"51395\n"},
{ &xtra[183], XTRA_DESCR, "Set expire at ms stamp.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if key expire time was set, *0* if not.\n"},
{ &xtra[185], XTRA_SHORT, "get the ms time left"},
{ &xtra[186], XTRA_USAGE, "pttl key\n"},
{ &xtra[187], XTRA_EXAMPLE, "> pttl k\n"
"51395\n"},
{ &xtra[188], XTRA_DESCR, "Get expire time in milliseconds.\n"},
{ NULL, XTRA_RETURN,
"The amount of milliseconds left or *-1* when key doesn't expire, and\n"
"*-2* when key doesn't exist.\n"},
{ &xtra[190], XTRA_SHORT, "get a random key"},
{ &xtra[191], XTRA_USAGE, "randomkey \n"},
{ &xtra[192], XTRA_EXAMPLE, "> randomkey\n"
"\"bf\"\n"
"> randomkey\n"
"\"jjj\"\n"},
{ &xtra[193], XTRA_DESCR, "Get a random key.\n"},
{ NULL, XTRA_RETURN, "A string key, if any exists, *nil* if not.\n"},
{ &xtra[195], XTRA_SHORT, "rename key to new name"},
{ &xtra[196], XTRA_USAGE, "rename key newkey\n"},
{ &xtra[197], XTRA_EXAMPLE, "> rename jjj j\n"
"'OK'\n"},
{ &xtra[198], XTRA_DESCR, "Rename key to a new name.\n"},
{ NULL, XTRA_RETURN, "*OK* when successful, error otherwise.\n"},
{ &xtra[200], XTRA_SHORT, "rename key if not exist"},
{ &xtra[201], XTRA_USAGE, "renamenx key newkey\n"},
{ &xtra[202], XTRA_EXAMPLE, "> renamenx k j\n"
"0\n"
"> del j\n"
"1\n"
"> renamenx k j\n"
"1\n"},
{ &xtra[203], XTRA_DESCR, "Rename key if new key doesn't exists.\n"},
{ NULL, XTRA_RETURN,
"An integer *1* if success, *0* if not, error if key doesn't exist.\n"},
{ &xtra[205], XTRA_SHORT, "set last access time"},
{ &xtra[206], XTRA_USAGE, "touch key [key ...]\n"},
{ &xtra[207], XTRA_EXAMPLE, "> touch j\n"
"1\n"
"> object idletime j\n"
"4\n"},
{ &xtra[208], XTRA_DESCR, "Set update time of key.\n"},
{ NULL, XTRA_RETURN, "An integer *1* if success, *0* if not.\n"},
{ &xtra[210], XTRA_SHORT, "get ttl secs"},
{ &xtra[211], XTRA_USAGE, "ttl key\n"},
{ &xtra[212], XTRA_EXAMPLE, "> ttl j\n"
"-1\n"
"> expire j 100\n"
"1\n"
"> ttl j\n"
"98\n"},
{ &xtra[213], XTRA_DESCR, "Get expire time in seconds.\n"},
{ NULL, XTRA_RETURN,
"The amount of seconds left or *-1* when key doesn't expire, and *-2*\n"
"when key doesn't exist.\n"},
{ &xtra[215], XTRA_SHORT, "get type of key"},
{ &xtra[216], XTRA_USAGE, "type key\n"},
{ &xtra[217], XTRA_EXAMPLE,
"> pfadd hl 1 2 3 4 5 6 10\n"
"1\n"
"> type hl\n"
"'hyperloglog'\n"},
{ &xtra[218], XTRA_DESCR, "Get the type of a key.\n"},
{ NULL, XTRA_RETURN,
"The type of key in a string or the string none.\n"},
{ &xtra[220], XTRA_SHORT, "delete key"},
{ &xtra[221], XTRA_USAGE, "unlink key [key ...]\n"},
{ &xtra[222], XTRA_EXAMPLE, "> unlink j k l m\n"
"2\n"},
{ &xtra[223], XTRA_DESCR, "Non-blocking delete, mark deleted.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of keys deleted is returned.\n"},
{ &xtra[225], XTRA_SHORT, "scan matching keys"},
{ &xtra[226], XTRA_USAGE, "scan curs [match pat] [count int]\n"},
{ &xtra[227], XTRA_EXAMPLE,
"> scan 0 match j* count 1\n"
"[\"18521679\",[\"jjj\"]]\n"
"> scan 18521679 match j* count 1\n"
"[\"25519714\",[\"j\"]]\n"
"> scan 25519714 match j* count 1\n"
"[\"0\",[]]\n"},
{ &xtra[228], XTRA_DESCR,
"Get the keys which match a pattern.  The cursor is the offset into the\n"
"scan where the results will start.  If the cursor is equal to 18521679\n"
"from the example, then the match will start at the 18521679th element\n"
"in the key hash.\n"},
{ NULL, XTRA_RETURN,
"An array within an array.  The outer array is the cursor counter, the\n"
"inner array is the keys matched.\n"},
{ &xtra[230], XTRA_SHORT, "block and lpop list"},
{ &xtra[231], XTRA_USAGE, "blpop key [key ...] timeout\n"},
{ &xtra[232], XTRA_EXAMPLE, "> brpop l m 0\n"
"[\"l\",\"one\"]\n"},
{ &xtra[233], XTRA_DESCR,
"Left pop the list and return the pair of list name and value.  If the\n"
"list is empty, block and wait for timeout seconds.  When the timeout\n"
"is zero, the command blocks indefinitely.\n"},
{ NULL, XTRA_RETURN,
"An array with the name of the list follwed by the left most item\n"
"popped from the list.  If timeout expires, *null* is returned.\n"},
{ &xtra[235], XTRA_SHORT, "block and rpop list"},
{ &xtra[236], XTRA_USAGE, "brpop key [key ...] timeout\n"},
{ &xtra[237], XTRA_EXAMPLE, "> brpop l m 0\n"
"[\"l\",\"three\"]\n"},
{ &xtra[238], XTRA_DESCR,
"Right pop the list and return the pair of list name and value.  If the\n"
"list is empty, block and wait for timeout seconds.  When the timeout\n"
"is zero, the command blocks indefinitely.\n"},
{ NULL, XTRA_RETURN,
"An array with the name of the list follwed by the right most item\n"
"popped from the list.  If timeout expires, *null* is returned.\n"},
{ &xtra[240], XTRA_SHORT, "block rpop then lpush"},
{ &xtra[241], XTRA_USAGE, "brpoplpush src dest timeout\n"},
{ &xtra[242], XTRA_EXAMPLE,
"> lpush l one\n"
"1\n"
"> brpoplpush l m 0\n"
"\"one\"\n"
"> lrange l 0 -1\n"
"[]\n"
"> lrange m 0 -1\n"
"[\"one\"]\n"},
{ &xtra[243], XTRA_DESCR,
"Right pop the source list and left push the destination list.  If\n"
"source is empty, block and wait for an element.  The timeout is in\n"
"seconds and when it is zero, the command blocks indefinitely.\n"},
{ NULL, XTRA_RETURN,
"A string of the element transferred or *null* when timeout expires.\n"},
{ &xtra[245], XTRA_SHORT, "get list elem at index"},
{ &xtra[246], XTRA_USAGE, "lindex key idx\n"},
{ &xtra[247], XTRA_EXAMPLE, "> lindex m 0\n"
"\"one\"\n"
"> lindex m 10\n"
"nil\n"},
{ &xtra[248], XTRA_DESCR,
"Get list element at index.  Index starts at zero and ends at list\n"
"length - 1.\n"},
{ NULL, XTRA_RETURN,
"A string of the element at index or *nil* when index is out of range.\n"},
{ &xtra[250], XTRA_SHORT, "insert list elem"},
{ &xtra[251], XTRA_USAGE, "linsert key before | after piv val\n"},
{ &xtra[252], XTRA_EXAMPLE,
"> linsert m after two three\n"
"3\n"
"> linsert m after four five\n"
"-1\n"
"> lrange m 0 -1\n"
"[\"one\",\"two\",\"three\"]\n"},
{ &xtra[253], XTRA_DESCR,
"Insert into list an value before or after the element named by piv.\n"},
{ NULL, XTRA_RETURN,
"The count of elements in the list after inserting the new element, or\n"
"*-1* when the piv is not found.\n"},
{ &xtra[255], XTRA_SHORT, "get list length"},
{ &xtra[256], XTRA_USAGE, "llen key\n"},
{ &xtra[257], XTRA_EXAMPLE, "> llen m\n"
"3\n"},
{ &xtra[258], XTRA_DESCR, "Get list length.\n"},
{ NULL, XTRA_RETURN,
"The count of elements in list, *0* if list doesn't exist.\n"},
{ &xtra[260], XTRA_SHORT, "left pop list elem"},
{ &xtra[261], XTRA_USAGE, "lpop key\n"},
{ &xtra[262], XTRA_EXAMPLE,
"> lpush x one\n"
"1\n"
"> lpop x\n"
"\"one\"\n"
"> lpop x\n"
"nil\n"
"> type x\n"
"'none'\n"},
{ &xtra[263], XTRA_DESCR,
"Left pop the element from the list.  The list key is removed when\n"
"there are zero elements left.\n"},
{ NULL, XTRA_RETURN,
"The string value of the element or *nil* if list doesn't exist.\n"},
{ &xtra[265], XTRA_SHORT, "left push list elem"},
{ &xtra[266], XTRA_USAGE, "lpush key val [val ..]\n"},
{ &xtra[267], XTRA_EXAMPLE, "> lpush x one two three\n"
"3\n"},
{ &xtra[268], XTRA_DESCR, "Left push elements to the list.\n"},
{ NULL, XTRA_RETURN,
"The length of the list after elements are added.\n"},
{ &xtra[270], XTRA_SHORT, "left push if exists"},
{ &xtra[271], XTRA_USAGE, "lpushx key val [val ..]\n"},
{ &xtra[272], XTRA_EXAMPLE,
"> lpushx y one two three\n"
"0\n"
"> type y\n"
"'none'\n"
"> lpushx x one two three\n"
"6\n"},
{ &xtra[273], XTRA_DESCR,
"Left push elements to the list only if it exists.\n"},
{ NULL, XTRA_RETURN,
"The length of the list after elements are added.\n"},
{ &xtra[275], XTRA_SHORT, "get range of elems"},
{ &xtra[276], XTRA_USAGE, "lrange key start stop\n"},
{ &xtra[277], XTRA_EXAMPLE,
"> lrange x 0 -1\n"
"[\"three\",\"two\",\"one\",\"three\",\"two\",\"one\"]\n"
"> lrange x 6 10\n"
"[]\n"},
{ &xtra[278], XTRA_DESCR,
"Get a range of elements from the list.  If list doesn't exist or range\n"
"has zero elements, an empty array is returned.  A negative index\n"
"starts from the end of the list:  *-1* is the last element, *-2* is\n"
"the second last element.\n"},
{ NULL, XTRA_RETURN,
"An array of elements between the start and stop indexes, inclusive.\n"},
{ &xtra[280], XTRA_SHORT, "remove count elems"},
{ &xtra[281], XTRA_USAGE, "lrem key count value\n"},
{ &xtra[282], XTRA_EXAMPLE,
"> lrange x 0 -1\n"
"[\"three\",\"two\",\"one\",\"three\",\"two\",\"one\"]\n"
"> lrem x 1 one\n"
"1\n"
"> lrem x 1 one\n"
"1\n"
"> lrem x 1 one\n"
"0\n"
"> lrange x 0 -1\n"
"[\"three\",\"two\",\"three\",\"two\"]\n"},
{ &xtra[283], XTRA_DESCR,
"Remove count list elements which match the value.\n"},
{ NULL, XTRA_RETURN, "An integer count of elements removed.\n"},
{ &xtra[285], XTRA_SHORT, "set list elem at index"},
{ &xtra[286], XTRA_USAGE, "lset key idx value\n"},
{ &xtra[287], XTRA_EXAMPLE,
"> lpush l one two three\n"
"3\n"
"> lset l 1 TWO\n"
"'OK'\n"
"> lrange l 0 -1\n"
"[\"three\",\"TWO\",\"one\"]\n"},
{ &xtra[288], XTRA_DESCR,
"Set the list element at index.  Index is zero based.\n"},
{ NULL, XTRA_RETURN,
"*OK* if success, error if out of range or not found.\n"},
{ &xtra[290], XTRA_SHORT, "trim list elems"},
{ &xtra[291], XTRA_USAGE, "ltrim key start stop\n"},
{ &xtra[292], XTRA_EXAMPLE,
"> lpush x one two three one two three\n"
"6\n"
"> ltrim x 4 10\n"
"'OK'\n"
"> lrange x 0 -1\n"
"[\"two\",\"one\"]\n"},
{ &xtra[293], XTRA_DESCR,
"Trim list to range.  If start is beyond the end of the list, then all\n"
"elements are removed.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[295], XTRA_SHORT, "right pop list elem"},
{ &xtra[296], XTRA_USAGE, "rpop key\n"},
{ &xtra[297], XTRA_EXAMPLE,
"> rpush x two three\n"
"2\n"
"> rpop x\n"
"\"three\"\n"
"> rpop x\n"
"\"two\"\n"
"> rpop x\n"
"nil\n"
"> type x\n"
"'none'\n"},
{ &xtra[298], XTRA_DESCR,
"Right pop the element from the list.  The list key is removed when\n"
"there are zero elements left.\n"},
{ NULL, XTRA_RETURN,
"The string value of the element or *nil* if list doesn't exist.\n"},
{ &xtra[300], XTRA_SHORT, "right pop left push"},
{ &xtra[301], XTRA_USAGE, "rpoplpush src dest\n"},
{ &xtra[302], XTRA_EXAMPLE,
"> rpush x two three\n"
"2\n"
"> rpoplpush x y\n"
"\"three\"\n"
"> rpoplpush x y\n"
"\"two\"\n"
"> rpoplpush x y\n"
"nil\n"},
{ &xtra[303], XTRA_DESCR,
"Right pop the source list and left push the destination list.  If\n"
"source is empty, return *nil*.\n"},
{ NULL, XTRA_RETURN,
"A string of the element transferred or *nil*.\n"},
{ &xtra[305], XTRA_SHORT, "right push elem"},
{ &xtra[306], XTRA_USAGE, "rpush key [val ...]\n"},
{ &xtra[307], XTRA_EXAMPLE, "> rpush x one two three\n"
"3\n"},
{ &xtra[308], XTRA_DESCR, "Right push elements to the list.\n"},
{ NULL, XTRA_RETURN,
"The length of the list after elements are added.\n"},
{ &xtra[310], XTRA_SHORT, "right push if exist"},
{ &xtra[311], XTRA_USAGE, "rpushx key [val ...]\n"},
{ &xtra[312], XTRA_EXAMPLE,
"> rpushx y one two three\n"
"0\n"
"> type y\n"
"'none'\n"
"> rpush x one two three\n"
"3\n"
"> rpushx x one two three\n"
"6\n"},
{ &xtra[313], XTRA_DESCR,
"Right push elements to the list only if it exists.\n"},
{ NULL, XTRA_RETURN,
"The length of the list after elements are added.\n"},
{ &xtra[315], XTRA_SHORT, "pattern subscribe"},
{ &xtra[316], XTRA_USAGE, "psubscribe pat [pat ...]\n"},
{ &xtra[317], XTRA_EXAMPLE,
"> psubscribe p* q*\n"
"executing: [\"psubscribe\",\"p*\",\"q*\"]\n"
"[\"psubscribe\",\"p*\",1]\n"
"[\"psubscribe\",\"q*\",2]\n"
"[\"pmessage\",\"p*\",\"publius\",\"friend\"]\n"
"[\"pmessage\",\"q*\",\"qQq\",\"OoO\"]\n"},
{ &xtra[318], XTRA_DESCR,
"Subscribe to patterns.  The messages published to channels which match\n"
"the pattern will be forwarded to the client.  A message may be\n"
"forwarded to the same client more than once if its channel matches\n"
"multiple patterns.  It is also possible to be forwarded the message\n"
"again if the channel matches a subscription.\n"},
{ NULL, XTRA_RETURN,
"An array with the subscription and an integer which is a count of the\n"
"subscriptions open.\n"},
{ &xtra[320], XTRA_SHORT, "query pubsub info"},
{ &xtra[321], XTRA_USAGE,
"pubsub [channels [pattern] | numsub channel | numpat]\n"},
{ &xtra[322], XTRA_EXAMPLE,
"> pubsub channels\n"
"[\"two\",\"five\",\"seven\",\"six\",\"one\",\"three\",\"four\"]\n"
"> pubsub channels t*\n"
"[\"two\",\"three\"]\n"
"> pubsub channels x*\n"
"[]\n"},
{ &xtra[323], XTRA_DESCR,
"Get the channels that are in use which match a pattern.\n"},
{ &xtra[324], XTRA_EXAMPLE,
"> pubsub numsub one two three\n"
"[\"one\",1,\"two\",1,\"three\",1]\n"},
{ &xtra[325], XTRA_DESCR,
"Get the number of subscription on each channel.\n"},
{ &xtra[326], XTRA_EXAMPLE, "> pubsub numpat\n"
"3\n"},
{ NULL, XTRA_DESCR,
"Display the number of pattern subscriptions open.\n"},
{ &xtra[328], XTRA_SHORT, "publish msg to channel"},
{ &xtra[329], XTRA_USAGE, "publish channel msg\n"},
{ &xtra[330], XTRA_EXAMPLE, "> publish one two\n"
"1\n"},
{ &xtra[331], XTRA_DESCR, "Publish msg to channel.\n"},
{ NULL, XTRA_RETURN,
"A count of the times the message is forwarded to a client.\n"},
{ &xtra[333], XTRA_SHORT, "pattern unsubscribe"},
{ &xtra[334], XTRA_USAGE, "punsubscribe [pat ...]\n"},
{ &xtra[335], XTRA_EXAMPLE,
"> psubscribe x*\n"
"[\"psubscribe\",\"x*\",1]\n"
"> punsubscribe x*\n"
"[\"punsubscribe\",\"x*\",0]\n"},
{ &xtra[336], XTRA_DESCR,
"Unsubscribe patterns.  This cancels interest in the patterns\n"
"previously subscribed.  If no patterns are named, all patterns are\n"
"unsubscribed.\n"},
{ NULL, XTRA_RETURN,
"An array with the punsubscribe and an integer count of the number of\n"
"subscriptions that the client has open.\n"},
{ &xtra[338], XTRA_SHORT, "subscribe channel"},
{ &xtra[339], XTRA_USAGE, "subscribe chan [chan ...]\n"},
{ &xtra[340], XTRA_EXAMPLE, "> subscribe x\n"
"[\"subscribe\",\"x\",1]\n"},
{ &xtra[341], XTRA_DESCR,
"Subscribe to channels.  The messages published to the channels that\n"
"are subscribed will be forwarded to the client.\n"},
{ NULL, XTRA_RETURN,
"An array with the subscription and an integer which is a count of the\n"
"subscriptions open.\n"},
{ &xtra[343], XTRA_SHORT, "unsubscribe channel"},
{ &xtra[344], XTRA_USAGE, "unsubscribe [chan ...]\n"},
{ &xtra[345], XTRA_EXAMPLE,
"> subscribe x\n"
"[\"subscribe\",\"x\",1]\n"
"> unsubscribe x\n"
"[\"unsubscribe\",\"x\",0]\n"},
{ &xtra[346], XTRA_DESCR,
"Unsubscribe channels.  This cancels interest in the channels\n"
"previously subscribed.  If no channels are named, all channels are\n"
"unsubscribed.\n"},
{ NULL, XTRA_RETURN,
"An array with the unsubscribe and an integer count of the number of\n"
"subscriptions that the client has open.\n"},
{ &xtra[348], XTRA_SHORT, "query connected clients"},
{ &xtra[349], XTRA_USAGE,
"client [setname name | getname | id | kill match | list match |\n"
"        reply state ]\n"},
{ &xtra[350], XTRA_EXAMPLE, "> client setname AAA\n"
"'OK'\n"},
{ &xtra[351], XTRA_DESCR, "Set the current connection name.\n"},
{ &xtra[352], XTRA_EXAMPLE, "> client getname\n"
"\"AAA\"\n"},
{ &xtra[353], XTRA_DESCR, "Get the current connection name.\n"},
{ &xtra[354], XTRA_EXAMPLE, "> client id\n"
"12\n"},
{ &xtra[355], XTRA_DESCR, "Get the current connection id.\n"},
{ &xtra[356], XTRA_EXAMPLE,
"> client list type tcp\n"
"id=12 addr=[::1]:58806 fd=14 name=AAA kind=redis age=150 idle=0 rbuf=0 \n"
"  rsz=16384 imsg=7 br=257 wbuf=0 wsz=5120 omsg=6 bs=1082 flags=N db=0 \n"
"  sub=0 psub=0 multi=-1 cmd=client\n"
"id=13 addr=[::1]:58808 fd=15 name= kind=redis age=74 idle=74 rbuf=0 \n"
"  rsz=16384 imsg=0 br=0 wbuf=0 wsz=5120 omsg=0 bs=0 flags=N db=0 \n"
"  sub=0 psub=0 multi=-1 cmd=none\n"},
{ &xtra[357], XTRA_DESCR,
"List the clients.  The filters that match the clients which are listed\n"
"are:\n"
"\n"
"- type [ tcp | udp | unix | listen | redis | pubsub | normal | http |\n"
"  nats | capr | rv ] -- filter by connection class.\n"
"- id N -- filter by id number\n"
"- addr IP -- filter by IP address\n"
"- skipme -- skip the current connection\n"},
{ &xtra[358], XTRA_EXAMPLE,
"> client kill id 13\n"
"1\n"
"> client kill type redis skipme\n"
"0\n"},
{ &xtra[359], XTRA_DESCR,
"Kill clients.  The filters that match the clients which are listed the\n"
"same as the filters that list.\n"},
{ &xtra[360], XTRA_EXAMPLE,
"> client reply skip\n"
"> ping\n"
"> ping\n"
"'PONG'\n"
"> client reply off\n"
"> ping\n"
"> client reply on\n"
"'OK'\n"
"> ping\n"
"'PONG'\n"},
{ NULL, XTRA_DESCR,
"Alter the reply behavior of the connection.  If skipped or off, output\n"
"will be muted.\n"},
{ &xtra[362], XTRA_SHORT, "query command info"},
{ &xtra[363], XTRA_USAGE,
"command [count | getkeys <cmd-full> | info [cmd] | help]\n"},
{ &xtra[364], XTRA_EXAMPLE,
"> command info get\n"
"[[\"get\",2,['readonly','fast'],1,1,1]]\n"
"> command count\n"
"193\n"
"> command help get\n"
"['GET','key ; Get value']\n"},
{ NULL, XTRA_DESCR, "Get command details and help.\n"},
{ &xtra[366], XTRA_SHORT, "query config"},
{ &xtra[367], XTRA_USAGE,
"config [get <param> | rewrite | set <param> <value> | resetstat]\n"},
{ NULL, XTRA_DESCR,
"Get, set config parameters.  There is no configuration, so these\n"
"are not functional.\n"},
{ &xtra[369], XTRA_SHORT, "get number of keys"},
{ &xtra[370], XTRA_USAGE, "dbsize \n"},
{ &xtra[371], XTRA_EXAMPLE, "> dbsize\n"
"299999\n"},
{ NULL, XTRA_DESCR, "Get number of keys in db.\n"},
{ &xtra[373], XTRA_SHORT, "query server stats"},
{ &xtra[374], XTRA_USAGE,
"info [server | clients | memory | persistence | stats |\n"
"      replication | cpu | commandstats | cluster | keyspace]\n"},
{ &xtra[375], XTRA_EXAMPLE,
"> info server\n"
"raids_version:        1.0.0-11\n"
"raids_git:            fee49cdd\n"
"gcc_version:          9.1.1\n"
"process_id:           32167\n"
"> info clients\n"
"redis_clients:        1\n"
"pubsub_clients:       0\n"
"> info memory\n"
"vm_peak:              8204MB\n"
"vm_size:              8204MB\n"
"> info stats\n"
"ht_operations:        505M\n"
"ht_chains:            1.0\n"
"ht_read:              505M\n"
"ht_write:             287\n"
"> info cpu\n"
"used_cpu_sys:         0.069951\n"
"used_cpu_user:        1.445196\n"
"used_cpu_total:       1.515147\n"},
{ NULL, XTRA_DESCR, "Get version info and counters.\n"},
{ &xtra[377], XTRA_SHORT, "monitor commands"},
{ &xtra[378], XTRA_USAGE, "monitor \n"},
{ &xtra[379], XTRA_EXAMPLE,
"> monitor\n"
"[\"pmessage\",\"__monitor_@*\",\"__monitor_@0__:127.0.0.1:60646\",\n"
"[[\"get\",\"k\"],\"value\",\"1580125101.975300\"]]\n"},
{ &xtra[380], XTRA_DESCR,
"Monitor commands executed by server from all clients.  This is an\n"
"alias for `psubscribe _monitor_@*`.  Running once enables, running\n"
"twice disables the monitor.  In order to monitor just one client, or\n"
"just one ip address, use a pattern that includes the address:\n"
"`psubscribe _monitor_@0__:127.0.0.1*`\n"},
{ NULL, XTRA_RETURN,
"An array which indicates the db number and client connected address\n"
"(`\"__monitor_@0__:127.0.0.1:60646\"`), the command executed\n"
"(`[\"get\",\"k\"]`), and the result of executing the command (`\"value\"`).\n"},
{ &xtra[382], XTRA_SHORT, "shutdown server"},
{ &xtra[383], XTRA_USAGE, "shutdown\n"},
{ NULL, XTRA_DESCR,
"Shutdown server.  This will cause the connected instance to quit.\n"},
{ &xtra[385], XTRA_SHORT, "get server time"},
{ &xtra[386], XTRA_USAGE, "time \n"},
{ &xtra[387], XTRA_EXAMPLE, "> time\n"
"[\"1580125969\",\"562228\"]\n"},
{ &xtra[388], XTRA_DESCR, "Get server time.\n"},
{ NULL, XTRA_RETURN,
"An array, UTC seconds and microseconds (1 usec = 1/1000000 second).\n"},
{ &xtra[390], XTRA_SHORT, "add to set"},
{ &xtra[391], XTRA_USAGE, "sadd key mem [mem ...]\n"},
{ &xtra[392], XTRA_EXAMPLE, "> sadd s x y z\n"
"3\n"
"> sadd s z a b\n"
"2\n"},
{ &xtra[393], XTRA_DESCR, "Add one or more members to a set.\n"},
{ NULL, XTRA_RETURN, "An integer count of unique members added.\n"},
{ &xtra[395], XTRA_SHORT, "get member count"},
{ &xtra[396], XTRA_USAGE, "scard key\n"},
{ &xtra[397], XTRA_EXAMPLE, "> sadd s x y z\n"
"3\n"
"> scard s\n"
"3\n"},
{ &xtra[398], XTRA_DESCR, "Get the count of set members.\n"},
{ NULL, XTRA_RETURN, "An integer count of members.\n"},
{ &xtra[400], XTRA_SHORT, "subtract sets"},
{ &xtra[401], XTRA_USAGE, "sdiff key [key ...]\n"},
{ &xtra[402], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sdiff s t\n"
"[\"y\"]\n"
"> sdiff t s\n"
"[\"Y\"]\n"},
{ &xtra[403], XTRA_DESCR,
"Subtract sets.  Remove members of the first set using the members of\n"
"the the other sets.\n"},
{ NULL, XTRA_RETURN, "An array of members.\n"},
{ &xtra[405], XTRA_SHORT, "subtract and store"},
{ &xtra[406], XTRA_USAGE, "sdiffstore dest key [key ...]\n"},
{ &xtra[407], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sdiffstore sd s t\n"
"1\n"
"> smembers sd\n"
"[\"y\"]\n"
"> sdiffstore td t s\n"
"1\n"
"> smembers td\n"
"[\"Y\"]\n"},
{ &xtra[408], XTRA_DESCR,
"Remove members of the first set using the members of the the other\n"
"sets.  Store the result in the dest key.\n"},
{ NULL, XTRA_RETURN,
"A count of members stored in the set at the dest key.\n"},
{ &xtra[410], XTRA_SHORT, "intersect sets"},
{ &xtra[411], XTRA_USAGE, "sinter key [key ...]\n"},
{ &xtra[412], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sinter s t\n"
"[\"x\", \"z\"]\n"},
{ &xtra[413], XTRA_DESCR,
"Intersect all the sets, only keep a member if it is in all sets.\n"},
{ NULL, XTRA_RETURN, "An array of members.\n"},
{ &xtra[415], XTRA_SHORT, "intersect and store"},
{ &xtra[416], XTRA_USAGE, "sinterstore key [key ...]\n"},
{ &xtra[417], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sinterstore si s t\n"
"2\n"
"> smembers si\n"
"[\"x\", \"z\"]\n"},
{ &xtra[418], XTRA_DESCR,
"Intersect all the sets and store the result in the dest key.\n"},
{ NULL, XTRA_RETURN,
"A count of members stored in the set at the dest key.\n"},
{ &xtra[420], XTRA_SHORT, "test membership"},
{ &xtra[421], XTRA_USAGE, "sismember key mem\n"},
{ &xtra[422], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sismember s x\n"
"1\n"
"> sismember s Y\n"
"0\n"},
{ &xtra[423], XTRA_DESCR, "Test whether member is present in a set.\n"},
{ NULL, XTRA_RETURN,
"A *1* is returned if it is a member, a *0* if not.\n"},
{ &xtra[425], XTRA_SHORT, "get all members"},
{ &xtra[426], XTRA_USAGE, "smembers key\n"},
{ &xtra[427], XTRA_EXAMPLE, "> sadd s x y z\n"
"3\n"
"> smembers s\n"
"[\"x\",\"y\",\"z\"]\n"},
{ &xtra[428], XTRA_DESCR, "Get all the members in a set.\n"},
{ NULL, XTRA_RETURN,
"An array of set members, which could be empty if the key doesn't\n"
"exist.\n"},
{ &xtra[430], XTRA_SHORT, "move member to set"},
{ &xtra[431], XTRA_USAGE, "smove src dest mem\n"},
{ &xtra[432], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> smove s t x\n"
"1\n"
"> smove s t Y\n"
"0\n"
"> smove s t a\n"
"0\n"
"> smove s t y\n"
"1\n"
"> smembers s\n"
"[\"z\"]\n"
"> smembers t\n"
"[\"x\",\"Y\",\"z\",\"y\"]\n"},
{ &xtra[433], XTRA_DESCR,
"Move a member to another set, which removes the member from the source\n"
"and only adds it the destination if it exists in the source.\n"},
{ NULL, XTRA_RETURN,
"A *1* is returned if the member exists in the source and is moved and\n"
"a *0* is returned if the member does not exist in the source.\n"},
{ &xtra[435], XTRA_SHORT, "remove rand members"},
{ &xtra[436], XTRA_USAGE, "spop key [count]\n"},
{ &xtra[437], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> spop s\n"
"[\"y\"]\n"
"> spop t 300\n"
"[\"x\",\"Y\",\"z\"]\n"
"> smembers s\n"
"[\"x\",\"z\"]\n"
"> smembers t\n"
"[]\n"
"> spop t\n"
"[]\n"},
{ &xtra[438], XTRA_DESCR,
"Remove count random members from the set and return them.\n"},
{ NULL, XTRA_RETURN,
"An array of members removed, which may be less than count if there are\n"
"not enough in the set to fulfill the total.\n"},
{ &xtra[440], XTRA_SHORT, "get rand members"},
{ &xtra[441], XTRA_USAGE, "srandmember key [count]\n"},
{ &xtra[442], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> srandmember s\n"
"[\"y\"]\n"
"> srandmember t 300\n"
"[\"x\",\"Y\",\"z\"]\n"},
{ &xtra[443], XTRA_DESCR,
"Get count random members from the set and return them.\n"},
{ NULL, XTRA_RETURN,
"An array of members removed, which may be less than count if there are\n"
"not enough in the set to fulfill the total.\n"},
{ &xtra[445], XTRA_SHORT, "remove members"},
{ &xtra[446], XTRA_USAGE, "srem key mem [mem ...]\n"},
{ &xtra[447], XTRA_EXAMPLE, "> sadd s x y z\n"
"3\n"
"> srem s y Y z\n"
"2\n"},
{ &xtra[448], XTRA_DESCR, "Remove one or more members from the set.\n"},
{ NULL, XTRA_RETURN, "An integer count of members removed.\n"},
{ &xtra[450], XTRA_SHORT, "union sets"},
{ &xtra[451], XTRA_USAGE, "sunion key [key ...]\n"},
{ &xtra[452], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sunion s t\n"
"[\"x\",\"y\",\"Y\",\"z\"]\n"},
{ &xtra[453], XTRA_DESCR, "Union sets and return members.\n"},
{ NULL, XTRA_RETURN, "An array of members.\n"},
{ &xtra[455], XTRA_SHORT, "union and store"},
{ &xtra[456], XTRA_USAGE, "sunionstore dest key [key ...]\n"},
{ &xtra[457], XTRA_EXAMPLE,
"> sadd s x y z\n"
"3\n"
"> sadd t x Y z\n"
"3\n"
"> sunionstore su s t\n"
"4\n"
"> smembers su\n"
"[\"x\",\"y\",\"Y\",\"z\"]\n"},
{ &xtra[458], XTRA_DESCR,
"Union all the sets and store the result in the dest key.\n"},
{ NULL, XTRA_RETURN,
"A count of members stored in the set at the dest key.\n"},
{ &xtra[460], XTRA_SHORT, "iterate and match"},
{ &xtra[461], XTRA_USAGE, "sscan key curs [match pattern] [count cnt]\n"},
{ &xtra[462], XTRA_EXAMPLE,
"> sadd s abc abb abd xyz zzz\n"
"5\n"
"> sscan s 0 match a* count 1\n"
"[\"2\",[\"abc\"]]\n"
"> sscan s 2 match a* count 1\n"
"[\"3\",[\"abb\"]]\n"
"> sscan s 3 match a* count 1\n"
"[\"4\",[\"abd\"]]\n"
"> sscan s 4 match a* count 1\n"
"[\"0\",[]]\n"
"> sscan s 0\n"
"[\"0\",[\"abc\",\"abb\",\"abd\",\"xyz\",\"zzz\"]]\n"
"> sscan s 5 match a* count 1\n"
"[\"0\",[]]\n"},
{ &xtra[463], XTRA_DESCR,
"Get the fields and values which match a pattern.  The cursor is the\n"
"offset into the scan where the results will start.  If the cursor is\n"
"equal to 3, then the match will start at the 3rd member of the set.\n"},
{ NULL, XTRA_RETURN,
"An array within an array.  The outer array is the cursor counter, the\n"
"inner array are the members matched.\n"},
{ &xtra[465], XTRA_SHORT, "add to zset"},
{ &xtra[466], XTRA_USAGE,
"zadd key [nx | xx] [ch] [incr] score mem [score mem ...]\n"},
{ &xtra[467], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c\n"
"3\n"
"> zrange z 0 -1 withscores\n"
"[\"a\",\"10\",\"b\",\"20\",\"c\",\"30\"]\n"
"> zadd z incr 1.1 a 2.2 b 3.3 c\n"
"\"33.3\"\n"
"> zrange z 0 -1 withscores\n"
"[\"a\",\"11.1\",\"b\",\"22.2\",\"c\",\"33.3\"]\n"
"> zadd z incr 1 a\n"
"\"12.1\"\n"
"> zadd z nx 10 a 40 d\n"
"1\n"
"> zrange z 0 -1 withscores\n"
"[\"a\",\"12.1\",\"b\",\"22.2\",\"c\",\"33.3\",\"d\",\"40\"]\n"},
{ &xtra[468], XTRA_DESCR,
"Add members to zset with score, optionally with a no exist (*nx*) flag\n"
"or must exist (*xx*) flag.  The *ch* flag alters the count to the\n"
"number members changed, otherwise it is the count of the number of\n"
"members created.  The *incr* flag causes the score to be added to the\n"
"existing instead of replacing it.  The score is stored as 64 bit\n"
"decimal number, which has a 16 digit precision and -383 to +384\n"
"exponent range.\n"
"https://en.wikipedia.org/wiki/Decimal64_floating-point_format\n"},
{ NULL, XTRA_RETURN,
"A count of members created, or changed when the *ch* flag is set.  If\n"
"incr used, then the result is the new score.  A string type is used\n"
"for scores since it is a decimal real.\n"},
{ &xtra[470], XTRA_SHORT, "get member count"},
{ &xtra[471], XTRA_USAGE, "zcard key\n"},
{ &xtra[472], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c\n"
"3\n"
"> zrange z 0 -1\n"
"[\"a\",\"b\",\"c\"]\n"
"> zcard z\n"
"3\n"},
{ &xtra[473], XTRA_DESCR, "Get a count of the number members.\n"},
{ NULL, XTRA_RETURN,
"An integer count, *0* if the key doesn't exist.\n"},
{ &xtra[475], XTRA_SHORT, "count within bounds"},
{ &xtra[476], XTRA_USAGE, "zcount key min max\n"},
{ &xtra[477], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c\n"
"3\n"
"> zcount z 10 30\n"
"3\n"
"> zcount z 15 30\n"
"2\n"},
{ &xtra[478], XTRA_DESCR, "Get number of members within a bounds.\n"},
{ NULL, XTRA_RETURN,
"An integer count, *0* if the key doesn't exist.\n"},
{ &xtra[480], XTRA_SHORT, "incr score"},
{ &xtra[481], XTRA_USAGE, "zincrby key incr mem\n"},
{ &xtra[482], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c\n"
"3\n"
"> zincrby z 1.5 a\n"
"\"21.5\"\n"
"> zrange z 0 -1 withscores\n"
"[\"b\",\"20\",\"a\",\"21.5\",\"c\",\"30\"]\n"
"> zincrby z 1.5 A\n"
"\"1.5\"\n"
"> zrange z 0 -1 withscores\n"
"[\"A\",\"1.5\",\"b\",\"20\",\"a\",\"21.5\",\"c\",\"30\"]\n"},
{ &xtra[483], XTRA_DESCR,
"Add a score to member, if member doesn't exists, create it.\n"},
{ NULL, XTRA_RETURN, "The score after incrementing it.\n"},
{ &xtra[485], XTRA_SHORT, "intersect and store"},
{ &xtra[486], XTRA_USAGE,
"zinterstore dest num key [key ...] [weights w [w ...]]\n"
" [aggregate sum|min|max]\n"},
{ &xtra[487], XTRA_EXAMPLE,
"> zadd z1 10 a 20 b 30 c\n"
"3\n"
"> zadd z2 40 d 30 c 10 b\n"
"3\n"
"> zinterstore z3 2 z1 z2\n"
"2\n"
"> zrange z3 0 -1 withscores\n"
"[\"b\",\"30\",\"c\",\"60\"]\n"},
{ &xtra[488], XTRA_DESCR,
"Intersect zsets and store in destination key.  The members which are\n"
"in both sets are combined into the destination set.  The score is a\n"
"combination of both members, depending on the weight given to each set\n"
"and how it is aggregated.  The default is weight 1 for each set (or no\n"
"weight), and aggregating by sum.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the members in the new set.\n"},
{ &xtra[490], XTRA_SHORT, "count lexical bounds"},
{ &xtra[491], XTRA_USAGE, "zlexcount key min max\n"},
{ &xtra[492], XTRA_EXAMPLE,
"> zadd z 10 a 10 b 10 c 10 d\n"
"4\n"
"> zlexcount z a d\n"
"4\n"
"> zcount z 10 10\n"
"4\n"},
{ &xtra[493], XTRA_DESCR,
"Count members in a zset within a lexical bounds, when all elements\n"
"have the same score (and this only works when all members have the\n"
"same score).\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of members between an inclusive lexical\n"
"bounds.\n"},
{ &xtra[495], XTRA_SHORT, "get members with range"},
{ &xtra[496], XTRA_USAGE, "zrange key start stop [withscores]\n"},
{ &xtra[497], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrange z 2 3 withscores\n"
"[\"c\",\"30\",\"d\",\"40\"]\n"
"> zrange z 0 -1\n"
"[\"a\",\"b\",\"c\",\"d\"]\n"},
{ &xtra[498], XTRA_DESCR,
"Get range of members between ranked indices.  The elements are sorted\n"
"by score, low to high, so the 0th indexed element is the lowest score\n"
"and ranked lowest.  The start and stop can be negative, which would\n"
"index from the highest score, so the -1 indexed element is the highest\n"
"score and ranked highest.\n"},
{ NULL, XTRA_RETURN,
"An array of members with scores if requested.\n"},
{ &xtra[500], XTRA_SHORT, "get with lex range"},
{ &xtra[501], XTRA_USAGE, "zrangebylex key min max [limit off cnt]\n"},
{ &xtra[502], XTRA_EXAMPLE,
"> zadd z 10 a 10 b 10 c 10 d\n"
"4\n"
"> zrangebylex z [a [b\n"
"[\"a\",\"b\"]\n"
"> zrangebylex z - +\n"
"[\"a\",\"b\",\"c\",\"d\"]\n"
"> zrangebylex z - + limit 0 1\n"
"[\"a\"]\n"
"> zrangebylex z - + limit 1 1\n"
"[\"b\"]\n"},
{ &xtra[503], XTRA_DESCR,
"Get members in a zset within a lexical bounds, when all elements have\n"
"the same score (and this only works when all members have the same\n"
"score).\n"},
{ NULL, XTRA_RETURN, "An array of members.\n"},
{ &xtra[505], XTRA_SHORT, "get rev lex range"},
{ &xtra[506], XTRA_USAGE, "zrevrangebylex key min max [limit off cnt]\n"},
{ &xtra[507], XTRA_EXAMPLE,
"> zadd z 10 a 10 b 10 c 10 d\n"
"4\n"
"> zrevrangebylex z [c [a\n"
"[\"c\",\"b\",\"a\"]\n"
"> zrevrangebylex z + -\n"
"[\"d\",\"c\",\"b\",\"a\"]\n"
"> zrevrangebylex z + - limit 0 1\n"
"[\"d\"]\n"
"> zrevrangebylex z + - limit 1 1\n"
"[\"c\"]\n"},
{ &xtra[508], XTRA_DESCR,
"Get members in a zset within a lexical bounds high to low, in reverse,\n"
"when all elements have the same score (and this only works when all\n"
"members have the same score).\n"},
{ NULL, XTRA_RETURN, "An array of members.\n"},
{ &xtra[510], XTRA_SHORT, "get range by score"},
{ &xtra[511], XTRA_USAGE, "zrangebyscore key min max [withscores]\n"},
{ &xtra[512], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrangebyscore z 10 30\n"
"[\"a\",\"b\",\"c\"]\n"
"> zrangebyscore z (1 [2 withscores\n"
"[\"b\",\"20\"]\n"},
{ &xtra[513], XTRA_DESCR,
"Get range of members between scores.  The start is the lowest score\n"
"and the stop is the highest score.  The members are returned from the\n"
"lowest to the highest, in score order.\n"},
{ NULL, XTRA_RETURN,
"An array of members, with scores if requested.\n"},
{ &xtra[515], XTRA_SHORT, "get rank of member"},
{ &xtra[516], XTRA_USAGE, "zrank key mem\n"},
{ &xtra[517], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrank z b\n"
"1\n"
"> zrank z c\n"
"2\n"
"> zrank z f\n"
"nil\n"},
{ &xtra[518], XTRA_DESCR,
"Get index of member, it's rank, where it is ordered by score.\n"},
{ NULL, XTRA_RETURN,
"An integer indicating rank of member, *nil* if member not found.\n"},
{ &xtra[520], XTRA_SHORT, "remove members"},
{ &xtra[521], XTRA_USAGE, "zrem key mem [mem ...]\n"},
{ &xtra[522], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrem z b c f\n"
"2\n"
"> zrange z 0 -1 withscores\n"
"[\"a\",\"10\",\"d\",\"40\"]\n"},
{ &xtra[523], XTRA_DESCR, "Remove members by name.\n"},
{ NULL, XTRA_RETURN,
"The integer *1* if removed, *0* if not removed.\n"},
{ &xtra[525], XTRA_SHORT, "remove by lex"},
{ &xtra[526], XTRA_USAGE, "zremrangebylex key min max\n"},
{ &xtra[527], XTRA_EXAMPLE,
"> zadd z 10 a 10 b 10 c 10 d\n"
"4\n"
"> zremrangebylex z [a [c\n"
"3\n"
"> zrange z 0 -1 withscores\n"
"[\"d\",\"10\"]\n"},
{ &xtra[528], XTRA_DESCR,
"Remove members from a zset within a lexical bounds high to low, in\n"
"reverse, when all elements have the same score (and this only works\n"
"when all members have the same score).\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of members removed.\n"},
{ &xtra[530], XTRA_SHORT, "remove by rank"},
{ &xtra[531], XTRA_USAGE, "zremrangebyrank key start stop\n"},
{ &xtra[532], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zremrangebyrank z 1 2\n"
"2\n"
"> zrange z 0 -1 withscores\n"
"[\"a\",\"10\",\"d\",\"40\"]\n"},
{ &xtra[533], XTRA_DESCR,
"Remove members from a zset by rank, which is the index of order that\n"
"they are sorted.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of members removed.\n"},
{ &xtra[535], XTRA_SHORT, "remove by score"},
{ &xtra[536], XTRA_USAGE, "zremrangebyscore key start stop\n"},
{ &xtra[537], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zremrangebyscore z 10 20\n"
"2\n"
"> zrange z 0 -1 withscores\n"
"[\"c\",\"30\",\"d\",\"40\"]\n"},
{ &xtra[538], XTRA_DESCR, "Remove members from a zset by score.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of members removed.\n"},
{ &xtra[540], XTRA_SHORT, "get reverse range"},
{ &xtra[541], XTRA_USAGE, "zrevrange key start stop [withscores]\n"},
{ &xtra[542], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrevrange z 2 3 withscores\n"
"[\"b\",\"20\",\"a\",\"10\"]\n"
"> zrevrange z 0 -1\n"
"[\"d\",\"c\",\"b\",\"a\"]\n"},
{ &xtra[543], XTRA_DESCR,
"Get range of members between ranks.  The start is the lowest index of\n"
"the reverse ordered zset, the stop is the highest.  The members are\n"
"returned from low to high using a reverse ordered zset.\n"},
{ NULL, XTRA_RETURN,
"An array of members with scores if requested.\n"},
{ &xtra[545], XTRA_SHORT, "get reverse score"},
{ &xtra[546], XTRA_USAGE,
"zrevrangebyscore key start stop [withscores] [limit off cnt]\n"},
{ &xtra[547], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zrevrangebyscore z +inf -inf withscores\n"
"[\"d\",\"40\",\"c\",\"30\",\"b\",\"20\",\"a\",\"10\"]\n"
"> zrevrangebyscore z 20 10 withscores\n"
"[\"b\",\"20\",\"a\",\"10\"]\n"
"> zrevrangebyscore z (20 [10 withscores\n"
"[\"a\",\"10\"]\n"},
{ &xtra[548], XTRA_DESCR,
"Get reverse range of members between scores.  The start is the highest\n"
"score and the stop is the lowest score.  The members are returned from\n"
"the highest to the lowest, in reverse score order.\n"},
{ NULL, XTRA_RETURN,
"An array of members with scores if requested.\n"},
{ &xtra[550], XTRA_SHORT, "get reverse rank"},
{ &xtra[551], XTRA_USAGE, "zrevrank key mem\n"},
{ &xtra[552], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"40\n"
"> zrevrank z b\n"
"20\n"
"> zrevrank z c\n"
"10\n"
"> zrevrank z f\n"
"nil\n"},
{ &xtra[553], XTRA_DESCR,
"Get inverse rank index of member by name.  If the order of the zset is\n"
"reversed, then this is the index of the member.\n"},
{ NULL, XTRA_RETURN,
"An integer indicating rank of member, *nil* if member is not found.\n"},
{ &xtra[555], XTRA_SHORT, "get score of member"},
{ &xtra[556], XTRA_USAGE, "zscore key mem\n"},
{ &xtra[557], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zscore z a\n"
"\"10\"\n"
"> zscore z d\n"
"\"40\"\n"},
{ &xtra[558], XTRA_DESCR, "Get score of a member.\n"},
{ NULL, XTRA_RETURN,
"A string which contains a decimal value for the score.\n"},
{ &xtra[560], XTRA_SHORT, "store a union"},
{ &xtra[561], XTRA_USAGE,
"zunionstore dest num key [key ...] [weights w [w ...]]\n"
" [aggregate sum|min|max]\n"},
{ &xtra[562], XTRA_EXAMPLE,
"> zadd z1 10 a 20 b 30 c\n"
"3\n"
"> zadd z2 40 d 30 c 10 b\n"
"3\n"
"> zunionstore z3 2 z1 z2\n"
"4\n"
"> zrange z3 0 -1 withscores\n"
"[\"a\",\"10\",\"b\",\"30\",\"d\",\"40\",\"c\",\"60\"]\n"},
{ &xtra[563], XTRA_DESCR,
"Union zsets and store in destination key.  The members which are in\n"
"both sets are combined into the destination set.  The score is a\n"
"combination of both members, depending on the weight given to each set\n"
"and how it is aggregated.  The default is weight 1 for each set (or no\n"
"weight), and aggregating by sum.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the members in the new set.\n"},
{ &xtra[565], XTRA_SHORT, "iterate a zset"},
{ &xtra[566], XTRA_USAGE, "zscan key curs [match pattern] [count cnt]\n"},
{ &xtra[567], XTRA_EXAMPLE,
"> zadd z 50 abc 40 abb 30 abd 20 xyz 10 zzz\n"
"5\n"
"> zscan z 0 match a* count 1\n"
"[\"3\",[\"abd\",\"30\"]]\n"
"> zscan z 3 match a* count 1\n"
"[\"4\",[\"abb\",\"40\"]]\n"
"> zscan z 4 match a* count 1\n"
"[\"0\",[\"abc\",\"50\"]]\n"
"> zscan z 5 match a* count 1\n"
"[\"0\",[]]\n"
"> zscan z 0\n"
"[\"0\",[\"zzz\",\"10\",\"xyz\",\"20\",\"abd\",\"30\",\"abb\",\"40\",\"abc\",\"50\"]]\n"
"> zscan z 5 match a* count 1\n"
"[\"0\",[]]\n"},
{ &xtra[568], XTRA_DESCR,
"Get the fields and values which match a pattern.  The cursor is the\n"
"offset into the scan where the results will start.  If the cursor is\n"
"equal to 3, then the match will start at the 3rd member of the zset.\n"},
{ NULL, XTRA_RETURN,
"An array within an array.  The outer array is the cursor counter, the\n"
"inner array are the members matched with their scores.\n"},
{ &xtra[570], XTRA_SHORT, "remove minimum score"},
{ &xtra[571], XTRA_USAGE, "zpopmin key [count]\n"},
{ &xtra[572], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zpopmin z 2\n"
"[\"a\",\"10\",\"b\",\"20\"]\n"
"> zpopmin z 20\n"
"[\"c\",\"30\",\"d\",\"40\"]\n"
"> zpopmin z 20\n"
"[]\n"},
{ &xtra[573], XTRA_DESCR,
"Remove up to count members the lowest scores of the zset.  If not\n"
"enough members are currently in the set, truncate count to the number\n"
"of members in the set.\n"},
{ NULL, XTRA_RETURN,
"An array with the set of members and the scores, in the order they\n"
"were popped, an empty array is returned when the key doesn't exist.\n"},
{ &xtra[575], XTRA_SHORT, "remove maximum score"},
{ &xtra[576], XTRA_USAGE, "zpopmax key [count]\n"},
{ &xtra[577], XTRA_EXAMPLE,
"> zadd z 10 a 20 b 30 c 40 d\n"
"4\n"
"> zpopmax z 2\n"
"[\"d\",\"40\",\"c\",\"30\"]\n"
"> zpopmax z 20\n"
"[\"b\",\"20\",\"a\",\"10\"]\n"
"> zpopmax z 20\n"
"[]\n"},
{ &xtra[578], XTRA_DESCR,
"Remove up to count members the highest scores of the zset.  If not\n"
"enough members are currently in the set, truncate count to the number\n"
"of members in the set.\n"},
{ NULL, XTRA_RETURN,
"An array with the set of members and the scores, in the order they\n"
"were popped, an empty array is returned when the key doesn't exist.\n"},
{ &xtra[580], XTRA_SHORT, "block pop min score"},
{ &xtra[581], XTRA_USAGE, "bzpopmin key [key ...] timeout\n"},
{ &xtra[582], XTRA_EXAMPLE,
"> bzpopmin z x 2\n"
"null\n"
"> bzpopmin z x 0\n"
"[\"z\",\"a\",\"10\"]\n"
"> bzpopmin z x 0\n"},
{ &xtra[583], XTRA_DESCR,
"Remove lowest score member of the zset.  If no members are currently\n"
"in the zset, block and wait timeout seconds for a publisher to add\n"
"them.  Wait indefinitely when timeout is zero.\n"},
{ NULL, XTRA_RETURN,
"An array with the zset name and the member with scores.  If timeout\n"
"occurs, then null is returned.\n"},
{ &xtra[585], XTRA_SHORT, "block pop max score"},
{ &xtra[586], XTRA_USAGE, "bzpopmax key [key ...] timeout\n"},
{ &xtra[587], XTRA_EXAMPLE,
"> bzpopmax z x 2\n"
"null\n"
"> bzpopmax z x 0\n"
"[\"z\",\"d\",\"40\"]\n"
"> bzpopmax z x 0\n"},
{ &xtra[588], XTRA_DESCR,
"Remove hightest score member of the zset.  If no members are currently\n"
"in the zset, block and wait timeout seconds for a publisher to add\n"
"them.  Wait indefinitely when timeout is zero.\n"},
{ NULL, XTRA_RETURN,
"An array with the zset name and the member with scores.  If timeout\n"
"occurs, then null is returned.\n"},
{ &xtra[590], XTRA_SHORT, "append to value"},
{ &xtra[591], XTRA_USAGE, "append key value\n"},
{ &xtra[592], XTRA_EXAMPLE,
"> append s string\n"
"5\n"
"> append s value\n"
"9\n"
"> get s\n"
"\"stringvalue\"\n"},
{ &xtra[593], XTRA_DESCR, "Append value to key.\n"},
{ NULL, XTRA_RETURN,
"The integer strlen of the key after value appended.\n"},
{ &xtra[595], XTRA_SHORT, "count bits"},
{ &xtra[596], XTRA_USAGE, "bitcount key [start end]\n"},
{ &xtra[597], XTRA_EXAMPLE,
"> set s string\n"
"'OK'\n"
"> bitcount s 0 0\n"
"5\n"
"> bitfield s get u8 0\n"
"115\n"
"> getrange s 0 0\n"
"\"s\"\n"},
{ &xtra[598], XTRA_DESCR,
"Count the bits in string from byte offset start to offset end,\n"
"inclusive.\n"},
{ NULL, XTRA_RETURN,
"An integer count of bits set in the byte range.\n"},
{ &xtra[600], XTRA_SHORT, "manipulate bits"},
{ &xtra[601], XTRA_USAGE,
"bitfield key [get type off] [set type off value]\n"
" [incrby type off incr] [overflow wrap | sat | fail]\n"},
{ &xtra[602], XTRA_EXAMPLE,
"> bitfield bf set u8 0 1 set u8 8 2 set u8 16 3 set u8 24 4\n"
"[0,0,0,0]\n"},
{ &xtra[603], XTRA_DESCR,
"Set integer value at bit offset off as type, where type is signed (iN)\n"
"or unsigned (uN) with bit size *1* through *63*, *64* for signed.\n"},
{ &xtra[604], XTRA_RETURN,
"An array of the integers that existed before setting the new values.\n"},
{ &xtra[605], XTRA_EXAMPLE,
"> bitfield bf get u8 0 get u8 8 get u8 16 get u8 24\n"
"[1,2,3,4]\n"},
{ &xtra[606], XTRA_DESCR,
"Get integer value at bit offset, using signed or unsigned type.\n"},
{ &xtra[607], XTRA_RETURN, "An array of integers that are requested.\n"},
{ &xtra[608], XTRA_EXAMPLE,
"> bitfield bf incrby u8 0 1 incrby u8 8 1 incrby u8 16 1 incrby u8 24 1\n"
"[2,3,4,5]\n"
"> bitfield bf incrby u8 0 255 overflow wrap\n"
"[1]\n"
"> bitfield bf incrby u8 8 255 overflow sat\n"
"[255]\n"
"> bitfield bf incrby u8 16 255 overflow fail\n"
"[nil]\n"},
{ &xtra[609], XTRA_DESCR,
"Increment integer values at bit offsets.  The overflow arument\n"
"modifies the behavior of increment in the case that the result wraps\n"
"around zero.  Wrap is the default, saturate (sat) caps the value at\n"
"the highest and lowest point, fail discards the new value and uses the\n"
"old value, returning *nil*.\n"},
{ NULL, XTRA_RETURN, "An array of the integers after incrementing.\n"},
{ &xtra[611], XTRA_SHORT, "bitwise operator"},
{ &xtra[612], XTRA_USAGE,
"bitop (and | or | xor | not) dest src [src src ...]\n"},
{ &xtra[613], XTRA_EXAMPLE,
"> bitfield i set u8 0 3\n"
"[0]\n"
"> bitfield j set u8 0 1\n"
"[0]\n"
"> bitop xor k j i\n"
"1\n"
"> bitfield k get u8 0\n"
"[2]\n"},
{ &xtra[614], XTRA_DESCR,
"Bitwise store to dest from srcs by performing logical operations on\n"
"each bit.\n"},
{ NULL, XTRA_RETURN,
"The number of bytes stored in dest, which is the minimum size of i and\n"
"j, since the trailing zeros are not stored.\n"},
{ &xtra[616], XTRA_SHORT, "find a bit"},
{ &xtra[617], XTRA_USAGE, "bitpos key bit [start end]\n"},
{ &xtra[618], XTRA_EXAMPLE,
"> bitfield k set u8 0 16\n"
"[0]\n"
"> bitpos k 1 0 1\n"
"4\n"
"> bitpos k 0 0 1\n"
"0\n"},
{ &xtra[619], XTRA_DESCR,
"Find first bit set or clear between start offset and end offset.\n"},
{ NULL, XTRA_RETURN,
"The position of the bit set or clear, or *-1* when not found.\n"},
{ &xtra[621], XTRA_SHORT, "decr by one"},
{ &xtra[622], XTRA_USAGE, "decr key\n"},
{ &xtra[623], XTRA_EXAMPLE, "> set k 10\n"
"'OK'\n"
"> decr k\n"
"9\n"
"> decr k\n"
"8\n"},
{ &xtra[624], XTRA_DESCR, "Decrement integer at key by one.\n"},
{ NULL, XTRA_RETURN,
"The value after decrementing it.  If key is created, then it is\n"
"initialized to *-1*.\n"},
{ &xtra[626], XTRA_SHORT, "decr by integer"},
{ &xtra[627], XTRA_USAGE, "decrby key int\n"},
{ &xtra[628], XTRA_EXAMPLE,
"> set k 10\n"
"'OK'\n"
"> decrby k -1\n"
"11\n"
"> decrby k 10\n"
"1\n"},
{ &xtra[629], XTRA_DESCR,
"Decrement integer at key by the integer argument.\n"},
{ NULL, XTRA_RETURN,
"The value after decrementing it.  If key is created, then it is\n"
"initialized to the negative of the integer argument.\n"},
{ &xtra[631], XTRA_SHORT, "get key value"},
{ &xtra[632], XTRA_USAGE, "get key\n"},
{ &xtra[633], XTRA_EXAMPLE,
"> set k val\n"
"'OK'\n"
"> get k\n"
"\"val\"\n"
"> del k\n"
"1\n"
"> get k\n"
"nil\n"},
{ &xtra[634], XTRA_DESCR, "Get the key value.\n"},
{ NULL, XTRA_RETURN, "The string value or *nil* when not found.\n"},
{ &xtra[636], XTRA_SHORT, "git bit at offset"},
{ &xtra[637], XTRA_USAGE, "getbit key off\n"},
{ &xtra[638], XTRA_EXAMPLE,
"> bitfield k set u8 0 16\n"
"[0]\n"
"> getbit k 0\n"
"0\n"
"> getbit k 3\n"
"0\n"
"> getbit k 4\n"
"1\n"},
{ &xtra[639], XTRA_DESCR,
"Get value at bit offset in the string stored at the key.\n"},
{ NULL, XTRA_RETURN,
"A *1* if the bit is set, a *0* if not set or not found.\n"},
{ &xtra[641], XTRA_SHORT, "get range in string"},
{ &xtra[642], XTRA_USAGE, "getrange key start end\n"},
{ &xtra[643], XTRA_EXAMPLE,
"> set k 0123456789\n"
"'OK'\n"
"> getrange k 3 7\n"
"\"34567\"\n"
"> getrange k 12 13\n"
"\"\"\n"
"> getrange k 7 -3\n"
"\"7\"\n"
"> getrange k 0 -1\n"
"\"0123456789\"\n"},
{ &xtra[644], XTRA_DESCR,
"Get a substring of the string value at key.  Start and/or end may be\n"
"negative to index from the end of the string.\n"},
{ NULL, XTRA_RETURN,
"A string with the characters in the range start to end, inclusive.\n"},
{ &xtra[646], XTRA_SHORT, "swap values"},
{ &xtra[647], XTRA_USAGE, "getset key value\n"},
{ &xtra[648], XTRA_EXAMPLE, "> getset k 2\n"
"nil\n"
"> getset k 3\n"
"\"2\"\n"},
{ &xtra[649], XTRA_DESCR,
"Swap new value with current value and return it.\n"},
{ NULL, XTRA_RETURN,
"The value currently stored with the key, or *nil* if the key is\n"
"created.\n"},
{ &xtra[651], XTRA_SHORT, "incr by one"},
{ &xtra[652], XTRA_USAGE, "incr key\n"},
{ &xtra[653], XTRA_EXAMPLE, "> incr k\n"
"4\n"
"> incr k\n"
"5\n"
"> del k\n"
"1\n"
"> incr k\n"
"1\n"},
{ &xtra[654], XTRA_DESCR, "Increment integer at key by one.\n"},
{ NULL, XTRA_RETURN,
"The value after it is incremented.  If key is created, then it is\n"
"initialized to *1*.\n"},
{ &xtra[656], XTRA_SHORT, "incr by integer"},
{ &xtra[657], XTRA_USAGE, "incrby key int\n"},
{ &xtra[658], XTRA_EXAMPLE,
"> del k\n"
"1\n"
"> incrby k 100\n"
"100\n"
"> incrby k 100\n"
"200\n"},
{ &xtra[659], XTRA_DESCR,
"Increment integer at key by the integer argument.\n"},
{ NULL, XTRA_RETURN,
"The value after it is incremented.  If key is created, then it is\n"
"initialized to the integer argument.\n"},
{ &xtra[661], XTRA_SHORT, "incr by decimal"},
{ &xtra[662], XTRA_USAGE, "incrbyfloat key decimal\n"},
{ &xtra[663], XTRA_EXAMPLE,
"> incrbyfloat n 1.1\n"
"\"1.1\"\n"
"> incrbyfloat n 1.1\n"
"\"2.2\"\n"
"> incrbyfloat n 1.1\n"
"\"3.3\"\n"},
{ &xtra[664], XTRA_DESCR,
"Increment number at key by decimal value.  This uses 128 bit decimal\n"
"arithmetic which has a 34 digit range and an exponent from -6143 to\n"
"+6144.  https://en.wikipedia.org/wiki/Decimal128_floating-point_format\n"},
{ NULL, XTRA_RETURN,
"The number after it is incremented.  If key is created, then it is\n"
"initialized to the decimal argument.\n"},
{ &xtra[666], XTRA_SHORT, "get multiple values"},
{ &xtra[667], XTRA_USAGE, "mget key [key ...]\n"},
{ &xtra[668], XTRA_EXAMPLE,
"> mset j 1 k 2 l 3 m 4\n"
"'OK'\n"
"> mget j k l m mm\n"
"[\"1\",\"2\",\"3\",\"4\",nil]\n"},
{ &xtra[669], XTRA_DESCR, "Get the values of multiple keys.\n"},
{ NULL, XTRA_RETURN,
"An array of values or nil when key doesn't exist or is not a string\n"
"type.\n"},
{ &xtra[671], XTRA_SHORT, "set multiple values"},
{ &xtra[672], XTRA_USAGE, "mset key val [key val ...]\n"},
{ &xtra[673], XTRA_EXAMPLE, "> mset j 1 k 2 l 3 m 4\n"
"'OK'\n"},
{ &xtra[674], XTRA_DESCR, "Set the values of multiple keys.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[676], XTRA_SHORT, "set if not exist"},
{ &xtra[677], XTRA_USAGE, "msetnx key val [key val ...]\n"},
{ &xtra[678], XTRA_EXAMPLE,
"> del j k l m\n"
"4\n"
"> msetnx j 1 k 2 l 3 m 4\n"
"1\n"
"> msetnx j 1 k 2 l 3 m 4\n"
"0\n"},
{ &xtra[679], XTRA_DESCR,
"Set the values of keys if all keys do not exist.\n"},
{ NULL, XTRA_RETURN,
"If all keys are created a *1* is returned, otherwise no kesy are\n"
"created and a *0* is returnd;\n"},
{ &xtra[681], XTRA_SHORT, "set with expiration"},
{ &xtra[682], XTRA_USAGE, "psetex key ms val\n"},
{ &xtra[683], XTRA_EXAMPLE,
"> psetex k 1580166892000 hello\n"
"'OK'\n"
"> ttl k\n"
"80\n"
"> psetex k 15000 hello\n"
"'OK'\n"
"> ttl k\n"
"13\n"
"> pttl k\n"
"11991\n"},
{ &xtra[684], XTRA_DESCR,
"Set the value with expiration of key in milliseconds.  The expiration\n"
"argument is either stamp or relative time.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[686], XTRA_SHORT, "set value"},
{ &xtra[687], XTRA_USAGE, "set key val [ex secs] [px ms] [nx | xx]\n"},
{ &xtra[688], XTRA_EXAMPLE,
"> set k one\n"
"'OK'\n"
"> set k one xx\n"
"'OK'\n"
"> set k one nx\n"
"nil\n"
"> set k one px 15000 xx\n"
"'OK'\n"
"> ttl k\n"
"11\n"
"> pttl k\n"
"4260\n"},
{ &xtra[689], XTRA_DESCR,
"Set the key value optionally with expiration and *nx* or *xx* test.\n"
"The *nx* option tests that the key does not exist before setting.  The\n"
"*xx* tests that the key does exist before setting.\n"},
{ NULL, XTRA_RETURN, "*OK* on success, *nil* when failed.\n"},
{ &xtra[691], XTRA_SHORT, "set bits"},
{ &xtra[692], XTRA_USAGE, "setbit key off 1 | 0\n"},
{ &xtra[693], XTRA_EXAMPLE,
"> setbit k 0 1\n"
"0\n"
"> setbit k 1 1\n"
"0\n"
"> setbit k 1 0\n"
"1\n"
"> getbit k 0\n"
"1\n"
"> getbit k 1\n"
"0\n"},
{ &xtra[694], XTRA_DESCR,
"Swap the value at bit offset in string with the new bit value.\n"},
{ NULL, XTRA_RETURN,
"A *1* or *0* is returned, the previous value held by bit.\n"},
{ &xtra[696], XTRA_SHORT, "set with expiration"},
{ &xtra[697], XTRA_USAGE, "setex key secs val\n"},
{ &xtra[698], XTRA_EXAMPLE,
"> setex k 1580166892 hello\n"
"'OK'\n"
"> ttl k\n"
"80\n"
"> setex k 15 hello\n"
"'OK'\n"
"> ttl k\n"
"13\n"
"> pttl k\n"
"11991\n"},
{ &xtra[699], XTRA_DESCR,
"Set the value with expiration of key in seconds.  The expiration\n"
"argument is either stamp or relative time.\n"},
{ NULL, XTRA_RETURN, "*OK*.\n"
"\n"},
{ &xtra[701], XTRA_SHORT, "set if not exist"},
{ &xtra[702], XTRA_USAGE, "setnx key val\n"},
{ &xtra[703], XTRA_EXAMPLE,
"> del k\n"
"1\n"
"> setnx k value\n"
"'OK'\n"
"> setnx k value\n"
"nil\n"},
{ &xtra[704], XTRA_DESCR, "Set the value if key does not exist.\n"},
{ NULL, XTRA_RETURN,
"*OK* if success, *nil* if key already exists.\n"},
{ &xtra[706], XTRA_SHORT, "set value range"},
{ &xtra[707], XTRA_USAGE, "setrange key off val\n"},
{ &xtra[708], XTRA_EXAMPLE,
"> del k\n"
"1\n"
"> setrange k 5 waterbuffalo\n"
"17\n"
"> get k\n"
"\"\\u0000\\u0000\\u0000\\u0000\\u0000waterbuffalo\"\n"},
{ &xtra[709], XTRA_DESCR,
"Overwrite or create a range in a string stored with the key at the\n"
"offset specified.  The data implicitly created by the setrange is zero\n"
"padded.\n"},
{ NULL, XTRA_RETURN, "The length of key after modification.\n"},
{ &xtra[711], XTRA_SHORT, "get strlen"},
{ &xtra[712], XTRA_USAGE, "strlen key\n"},
{ &xtra[713], XTRA_EXAMPLE,
"> del k\n"
"1\n"
"> setrange k 5 waterbuffalo\n"
"17\n"
"> strlen k\n"
"17\n"
"> get k\n"
"\"\\u0000\\u0000\\u0000\\u0000\\u0000waterbuffalo\"\n"},
{ &xtra[714], XTRA_DESCR,
"Get length of value, which is the maximum extent, not the strlen\n"
"function.\n"},
{ NULL, XTRA_RETURN,
"The length of value, which could be *0* if the key does not exist.\n"},
{ &xtra[716], XTRA_SHORT, "discard trans"},
{ &xtra[717], XTRA_USAGE, "discard \n"},
{ NULL, XTRA_DESCR, "Discard cmds issued after multi\n"},
{ &xtra[719], XTRA_SHORT, "run trans"},
{ &xtra[720], XTRA_USAGE, "exec \n"},
{ NULL, XTRA_DESCR, "Execute all commands after multi\n"},
{ &xtra[722], XTRA_SHORT, "start trans"},
{ &xtra[723], XTRA_USAGE, "multi \n"},
{ NULL, XTRA_DESCR, "Multi start transaction\n"},
{ &xtra[725], XTRA_SHORT, "stop watching"},
{ &xtra[726], XTRA_USAGE, "unwatch \n"},
{ NULL, XTRA_DESCR, "Forget watched keys\n"},
{ &xtra[728], XTRA_SHORT, "start watching"},
{ &xtra[729], XTRA_USAGE, "watch key [key ...]\n"},
{ NULL, XTRA_DESCR, "Watch keys to determine multi/exec blk\n"},
{ &xtra[731], XTRA_SHORT, "get stream info"},
{ &xtra[732], XTRA_USAGE,
"xinfo [consumers key groupname] [groups key] [stream key]\n"},
{ &xtra[733], XTRA_EXAMPLE,
"> xinfo groups S\n"
"[[\"name\",\"G\",\"consumers\",2,\"pending\",2,\"last-delivered-id\",\n"
"  \"1580474523226-0\"],\n"
"[\"name\",\"H\",\"consumers\",0,\"pending\",0,\"last-delivered-id\",\"0\"]]\n"},
{ &xtra[734], XTRA_DESCR,
"Get group info for a stream, which shows what the next *id* will be,\n"
"the number of consumers and the number of pending records.\n"},
{ &xtra[735], XTRA_RETURN,
"An array of groups where each group is an array of field values:\n"
"\n"
"- name -- the name of the group.\n"
"- consumers -- the count of consumers.\n"
"- pending -- the count of records in the pending queue.\n"
"- last-delivered-id -- an *id* pointer, used to incrementally send\n"
"  newer records to clients.\n"
"\n"
"The empty array *[]* is returned when the stream doesn't exist or it\n"
"does not have groups.\n"},
{ &xtra[736], XTRA_EXAMPLE,
"> xinfo stream S\n"
"[\"length\",3,\"groups\",2,\"last-generated-id\",\"1580474524828-0\",\"first-entry\",\n"
"[\"1580474520477-0\",[\"c\",\"1\"]],\"last-entry\",[\"1580474524828-0\",[\"b\",\"1\"]]]\n"},
{ &xtra[737], XTRA_DESCR, "Get the info for the a stream.\n"},
{ &xtra[738], XTRA_RETURN,
"An array of field values describing the stream:\n"
"\n"
"- length -- the number of items in the stream\n"
"- groups -- the number of groups associated with the stream\n"
"- last-generated-id -- the *id* generated by the stream, it is used to\n"
"  ensure that the next generated *id* is greater than the last.\n"
"- first-entry -- the item which is at the head of the stream.\n"
"- last-entry -- the item which is at the tail of the stream.\n"
"\n"
"The empty array *[]* is returned when the stream doesn't exist.\n"},
{ &xtra[739], XTRA_EXAMPLE,
"> xinfo consumers S G\n"
"[[\"name\",\"C\",\"pending\",1,\"idle\",2225891],[\"name\",\"D\",\"pending\",1,\n"
"  \"idle\",2221204]]\n"},
{ NULL, XTRA_RETURN,
"An array of consumers, which is an array of field values:\n"
"\n"
"- name -- the name of the consumer.\n"
"- pending -- the count of records in the pending queue for this\n"
"  consumer.\n"
"- idle -- the number of milliseconds since the consumer was active\n"
"\n"
"The empty array *[]* is returned when stream or group or consumers\n"
"don't exist.\n"},
{ &xtra[741], XTRA_SHORT, "add entry to stream"},
{ &xtra[742], XTRA_USAGE, "xadd key id field string [field string ...]\n"},
{ &xtra[743], XTRA_EXAMPLE,
"> xadd S * cosmic crisp\n"
"\"1580392806377-0\"\n"
"> xadd S * red delicous\n"
"\"1580392886784-0\"\n"
"> xrange S - +\n"
"[[\"1580392806377-0\",[\"cosmic\",\"crisp\"]],\n"
" [\"1580392886784-0\",[\"red\",\"delicious\"]]]\n"},
{ &xtra[744], XTRA_DESCR,
"Add field value pairs to stream.  If *id* is the star, then it is\n"
"generated using the current milliseconds, and an incrementing serial\n"
"number which is only incremented within the millisecond period.  If\n"
"the *id* is not generated, it should be one or two 64 bit integers\n"
"(separated by the dash) increasing in value, since an *id* must formed\n"
"to increase over time.\n"},
{ NULL, XTRA_RETURN,
"The string *id* associated with the new entry.\n"},
{ &xtra[746], XTRA_SHORT, "trim stream to size"},
{ &xtra[747], XTRA_USAGE, "xtrim key maxlen [~] count\n"},
{ &xtra[748], XTRA_EXAMPLE,
"> xadd S * cosmic crisp\n"
"\"1580392806377-0\"\n"
"> xadd S * red delicous\n"
"\"1580392886784-0\"\n"
"> xadd S * shiba inu\n"
"\"1580393902456-0\"\n"
"> xadd S * akita inu\n"
"\"1580393946076-0\"\n"
"> xlen S\n"
"4\n"
"> xtrim S maxlen 2\n"
"2\n"
"> xrange S - +\n"
"[[\"1580393902456-0\",[\"shiba\",\"inu\"]],\n"
" [\"1580393946076-0\",[\"akita\",\"inu\"]]\n"},
{ &xtra[749], XTRA_DESCR,
"Trims the stream to a maxlen items from the tail -- these are the\n"
"oldest entries.\n"},
{ NULL, XTRA_RETURN,
"An integer count of items removed from the stream.\n"},
{ &xtra[751], XTRA_SHORT, "delete from stream"},
{ &xtra[752], XTRA_USAGE, "xdel key id [id ...]\n"},
{ &xtra[753], XTRA_EXAMPLE,
"> xadd S * hello world\n"
"\"1580394876047-0\"\n"
"> xadd S * hello world\n"
"\"1580394877281-0\"\n"
"> xdel S 1580394876047-0 1580394877281-0\n"
"2\n"},
{ &xtra[754], XTRA_DESCR,
"Delete entries from the stream which match the *id* strings.\n"},
{ NULL, XTRA_RETURN,
"An integer count of items removed from the stream.\n"},
{ &xtra[756], XTRA_SHORT, "get range of items"},
{ &xtra[757], XTRA_USAGE, "xrange key start end [count cnt]\n"},
{ &xtra[758], XTRA_EXAMPLE,
"> xadd S * a 1\n"
"\"1580458726320-0\"\n"
"> xadd S * b 2\n"
"\"1580458729687-0\"\n"
"> xadd S * c 3\n"
"\"1580458733903-0\"\n"
"> xrange S - +\n"
"[[\"1580458726320-0\",[\"a\",\"1\"]],[\"1580458729687-0\",[\"b\",\"2\"]],\n"
" [\"1580458733903-0\",[\"c\",\"3\"]]]\n"
"> xrange S 1580458729687 1580458733903\n"
"[[\"1580458729687-0\",[\"b\",\"2\"]],[\"1580458733903-0\",[\"c\",\"3\"]]]\n"
"> xrange S 1580458733903-1 +\n"
"[]\n"},
{ &xtra[759], XTRA_DESCR,
"Get items between start and end range.  The *id* assigned to each item\n"
"in the stream is a numerically increasing value.  The start is the low\n"
"*id*, the end is the high *id*.  All of the items within the range are\n"
"returned, up to count, if specified.\n"},
{ NULL, XTRA_RETURN,
"An array of items.  Each item is an *id* followed by an array of all\n"
"of the fields defined in the entry.  The empty array is the result\n"
"when no items are found or the key does not exist.\n"},
{ &xtra[761], XTRA_SHORT, "get reverse range"},
{ &xtra[762], XTRA_USAGE, "xrevrange key start end [count cnt]\n"},
{ &xtra[763], XTRA_EXAMPLE,
"> xadd S * a 1\n"
"\"1580458726320-0\"\n"
"> xadd S * b 2\n"
"\"1580458729687-0\"\n"
"> xadd S * c 3\n"
"\"1580458733903-0\"\n"
"> xrevrange S + -\n"
"[[\"1580458733903-0\",[\"c\",\"3\"]],[\"1580458729687-0\",[\"b\",\"2\"]],\n"
" [\"1580458726320-0\",[\"a\",\"1\"]]]\n"
"> xrevrange S 1580458733903 1580458729687\n"
"[[\"1580458733903-0\",[\"c\",\"3\"]],[\"1580458729687-0\",[\"b\",\"2\"]]]\n"
"> xrevrange S + 1580458733903-1\n"
"[]\n"},
{ &xtra[764], XTRA_DESCR,
"Get items between start and end range.  The *id* assigned to each item\n"
"in the stream is a numerically increasing value.  The start is the\n"
"high *id*, the end is the low *id*.  All of the items within the range\n"
"are returned, up to count, if specified.\n"},
{ NULL, XTRA_RETURN,
"An array of items.  Each item is an *id* followed by an array of all\n"
"of the fields defined in the entry.  The empty array is the result\n"
"when no items are found or the key does not exist.\n"},
{ &xtra[766], XTRA_SHORT, "get stream length"},
{ &xtra[767], XTRA_USAGE, "xlen key\n"},
{ &xtra[768], XTRA_EXAMPLE,
"> xadd S * a 1\n"
"\"1580458726320-0\"\n"
"> xlen S\n"
"1\n"
"> xadd S * b 2\n"
"\"1580458729687-0\"\n"
"> xlen S\n"
"2\n"},
{ &xtra[769], XTRA_DESCR, "Get the number of items in a stream.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of items, or *0* when the stream is\n"
"empty or the key doesn't exist.\n"},
{ &xtra[771], XTRA_SHORT, "get next entry"},
{ &xtra[772], XTRA_USAGE,
"xread [count cnt] [block ms] streams key [key ...] id [id ...]\n"},
{ &xtra[773], XTRA_EXAMPLE,
"> xadd S * a 1\n"
"\"1580466119503-0\"\n"
"> xadd T * x 1\n"
"\"1580466126059-0\"\n"
"> xread streams S T 0 0\n"
"[[\"S\",[[\"1580466119503-0\",[\"a\",\"1\"]]]],\n"
" [\"T\",[[\"1580466126059-0\",[\"x\",\"1\"]]]]]\n"},
{ &xtra[774], XTRA_DESCR,
"Read new items on one or more streams which occur after the *id*.  If\n"
"all streams are exhausted of new items, then the command can block, if\n"
"requested, until items are available on any stream.\n"},
{ NULL, XTRA_RETURN,
"An array of streams names that have items, with the array of item\n"
"identfiers, then the array of field values.  The result is\n"
"structurally similar to the xrange command, where the results are\n"
"combined over multiple streams.  If no items are available, or the\n"
"block timeout expires, a *null* is returned.\n"},
{ &xtra[776], XTRA_SHORT, "modify group"},
{ &xtra[777], XTRA_USAGE,
"xgroup [create key grp id [mkstream]] [setid key grp id]\n"
" [destroy key grp] [delconsumer key grp cname]\n"},
{ &xtra[778], XTRA_EXAMPLE,
"> xgroup create S G 0 mkstream\n"
"'OK'\n"
"> xgroup create T G 0 mkstream\n"
"'OK'\n"
"> xadd S * a 1\n"
"\"1580466848853-0\"\n"
"> xadd T * x 1\n"
"\"1580466970348-0\"\n"
"> xreadgroup group G C streams S T > >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]],\n"
" [\"T\",[[\"1580466970348-0\",[\"x\",\"1\"]]]]]\n"},
{ &xtra[779], XTRA_DESCR,
"Create a consumer group and initialize the next id that consumers will\n"
"read.  The option mkstream will create an empty stream if it doesn't\n"
"exit.\n"},
{ &xtra[780], XTRA_EXAMPLE,
"> xgroup setid S G 0\n"
"'OK'\n"
"> xreadgroup group G D streams S T > >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]]]\n"},
{ &xtra[781], XTRA_DESCR,
"Set the next id that consumers will read.  Each group contains a\n"
"pointer to the next id.  The setid argument moves the pointer to a\n"
"different location.\n"},
{ &xtra[782], XTRA_EXAMPLE,
"> xinfo groups T\n"
"[[\"name\",\"G\",\"consumers\",1,\"pending\",1,\n"
"  \"last-delivered-id\",\"1580466970348-0\"]]\n"
"> xgroup destroy T G\n"
"1\n"
"> xinfo groups T\n"
"[]\n"},
{ &xtra[783], XTRA_DESCR,
"Destroy the group and associated consumers from the stream.\n"},
{ &xtra[784], XTRA_EXAMPLE,
"> xinfo consumers S G\n"
"[[\"name\",\"C\",\"pending\",1,\"idle\",466329],\n"
" [\"name\",\"D\",\"pending\",1,\"idle\",378057]]\n"
"> xgroup delconsumer S G D\n"
"1\n"
"> xinfo consumers S G\n"
"[[\"name\",\"C\",\"pending\",1,\"idle\",514499]]\n"},
{ &xtra[785], XTRA_DESCR,
"Delete the consumer from the group.  The removes any pending items\n"
"that the consumer has outstanding.\n"},
{ NULL, XTRA_RETURN,
"- xgroup create -- *OK* or error if key not found and mkstream not\n"
"  specified.\n"
"- xgroup setid -- *OK* or error if group or key not found.\n"
"- xgroup destroy -- An integer *1* if group destroyed, *0* if not\n"
"  found.\n"
"- xgroup delconsumer -- An integer *1* if consumer destroyed, *0* if\n"
"  not found.\n"},
{ &xtra[787], XTRA_SHORT, "read next group"},
{ &xtra[788], XTRA_USAGE,
"xreadgroup group grp consumer [count cnt] [block ms] [noack]\n"
" streams key [key ...] id [id ...]\n"},
{ &xtra[789], XTRA_EXAMPLE,
"> xadd S * a 1\n"
"\"1580466848853-0\"\n"
"> xadd T * x 1\n"
"\"1580466970348-0\"\n"
"> xreadgroup group G C streams S T > >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]],\n"
" [\"T\",[[\"1580466970348-0\",[\"x\",\"1\"]]]]]\n"
"> xpending S G\n"
"[1,\"1580466848853-0\",\"1580466848853-0\",[[\"C\",\"1\"]]]\n"
"> xpending T G\n"
"[1,\"1580466970348-0\",\"1580466970348-0\",[[\"C\",\"1\"]]]\n"
"> xack S G 1580466848853-0\n"
"1\n"
"> xack T G 1580466970348-0\n"
"1\n"
"> xpending S G\n"
"[0,nil,nil,nil]\n"
"> xpending T G\n"
"[0,nil,nil,nil]\n"},
{ &xtra[790], XTRA_DESCR,
"Read streams through consumer group.  This assigns items from the\n"
"stream to the consumer attached to the group.  A record of this read\n"
"is saved with the stream until it is acked with xack, or reassigned\n"
"with xclaim, or the group is destroyed with xgroup.  If the *id* used\n"
"to read the streams is *>*, then the next id pointer for the group is\n"
"used to read the next available items.  If the *id* is a number, then\n"
"the records returned are already assigned to the consumer.  This form\n"
"is used to find the records after they have been delivered.  If no\n"
"records are available and *>* is used, then *null* is returned or if\n"
"block ms is used, the command is blocked until the records become\n"
"available.\n"},
{ NULL, XTRA_RETURN,
"An array of streams names that have items, with the array of item\n"
"identfiers, then the array of field values.  The result is\n"
"structurally similar to the xrange command, where the results are\n"
"combined over multiple streams.  If no items are available, or the\n"
"block timeout expires, a *null* is returned.\n"},
{ &xtra[792], XTRA_SHORT, "consume group entry"},
{ &xtra[793], XTRA_USAGE, "xack key grp id [id ...]\n"},
{ &xtra[794], XTRA_EXAMPLE,
"> xreadgroup group G C streams S >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]]]\n"
"> xpending S G\n"
"[1,\"1580466848853-0\",\"1580466848853-0\",[[\"C\",\"1\"]]]\n"
"> xack S G 1580466848853-0\n"
"1\n"
"> xpending S G\n"
"[0,nil,nil,nil]\n"
"> xack S G 1580466848853-0\n"
"0\n"},
{ &xtra[795], XTRA_DESCR,
"Acknowledge consumption of the *id* strings.  The record in the stream\n"
"associated with the group is removed from the pending list.\n"},
{ NULL, XTRA_RETURN,
"An integer count of the number of *id* strings acked.\n"},
{ &xtra[797], XTRA_SHORT, "read old group entries"},
{ &xtra[798], XTRA_USAGE,
"xclaim key grp consumer min-idle-time id [id ...] [idle ms]\n"
"[time ms-utc] [retrycount cnt] [force] [justid]\n"},
{ &xtra[799], XTRA_EXAMPLE,
"> xreadgroup group G C streams S >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]]]\n"
"> xclaim S G D 1 1580466848853-0\n"
"[[\"1580466848853-0\",[\"a\",\"1\"]]]\n"
"> xpending S G\n"
"[1,\"1580466848853-0\",\"1580466848853-0\",[[\"D\",\"1\"]]]\n"
"> xack S G 1580466848853-0\n"
"1\n"
"> xpending S G\n"
"[0,nil,nil,nil]\n"
"> xgroup setid S G 0\n"
"'OK'\n"
"> xclaim S G D 1 1580466848853-0\n"
"[]\n"
"> xreadgroup group G C streams S >\n"
"[[\"S\",[[\"1580466848853-0\",[\"a\",\"1\"]]]]]\n"
"> xclaim S G D 10000 1580466848853-0\n"
"[]\n"
"> xclaim S G D 100 1580466848853-0\n"
"[[\"1580466848853-0\",[\"a\",\"1\"]]]\n"
"> xclaim S G E 0 1580466848853-0 force\n"
"[[\"1580466848853-0\",[\"a\",\"1\"]]]\n"
"> xclaim S G X 0 1580466848853-0 force justid\n"
"[\"1580466848853-0\"]\n"
"> xpending S G\n"
"[1,\"1580466848853-0\",\"1580466848853-0\",[[\"X\",\"1\"]]]\n"},
{ &xtra[800], XTRA_DESCR,
"Recover items from a stream, group and assign them to a consumer.  An\n"
"item must be older than min-idle-time millisecs in order for xclaim to\n"
"succeed.  This is to prevent the race condtion of claiming items that\n"
"were already claimed.  The force argument causes the record to be\n"
"claimed regardless of the pending state.\n"},
{ NULL, XTRA_RETURN,
"The records are returned when xclaim succeeds, unless justid is\n"
"specified.  An empty array is returned when no *id* succeeds.\n"},
{ &xtra[802], XTRA_SHORT, "find pending by group"},
{ &xtra[803], XTRA_USAGE,
"xpending key grp [start end count] [consumer]\n"},
{ &xtra[804], XTRA_EXAMPLE,
"> xpending S G\n"
"[1,\"1580466848853-0\",\"1580466848853-0\",[[\"X\",\"1\"]]]\n"
"> xreadgroup group G C count 1 streams S >\n"
"[[\"S\",[[\"1580472282479-0\",[\"b\",\"1\"]]]]]\n"
"> xreadgroup group G D count 1 streams S >\n"
"[[\"S\",[[\"1580472289614-0\",[\"c\",\"1\"]]]]]\n"
"> xpending S G\n"
"[3,\"1580466848853-0\",\"1580472289614-0\",\n"
" [[\"X\",\"1\"],[\"C\",\"1\"],[\"D\",\"1\"]]]\n"
"> xpending S G - + 10 C\n"
"[[\"1580472282479-0\",\"C\",73749,1]]\n"
"> xpending S G - + 10 D\n"
"[[\"1580472289614-0\",\"D\",77612,1]]\n"
"> xadd S * d 1\n"
"\"1580472723272-0\"\n"
"> xreadgroup group G D count 1 streams S >\n"
"[[\"S\",[[\"1580472723272-0\",[\"d\",\"1\"]]]]]\n"
"> xpending S G - + 10 D\n"
"[[\"1580472289614-0\",\"D\",397783,1],[\"1580472723272-0\",\"D\",4319,1]]\n"
"> xpending S G\n"
"[4,\"1580466848853-0\",\"1580472723272-0\",\n"
" [[\"X\",\"1\"],[\"C\",\"1\"],[\"D\",\"2\"]]]\n"},
{ &xtra[805], XTRA_DESCR,
"Get the pending *id* records from group.  Without a consumer, this\n"
"requests the number of pending records, pending head and tail for the\n"
"group, and the consumers pending count pairs.  With the consumer, this\n"
"requests all of the pending records for the consumer, up to the count\n"
"specified.  The id, the consumer, the pending time in milliseconds,\n"
"and the delivered count are in each of the pending records.\n"},
{ NULL, XTRA_RETURN,
"- xpending key grp -- An array of pending count, head pending *id*,\n"
"  tail pending *id*, and an array off consumers, each with a count of\n"
"  pending records.\n"
"- xpending key grp start end count consumer -- An array of pending\n"
"  records belonging to the consumer requested, each with an *id*, an\n"
"  idle time, and a delivery count.\n"},
{ &xtra[807], XTRA_SHORT, "set last entry of group"},
{ &xtra[808], XTRA_USAGE, "xsetid key grp id\n"},
{ &xtra[809], XTRA_EXAMPLE,
"> xinfo groups S\n"
"[[\"name\",\"G\",\"consumers\",0,\"pending\",0,\n"
"  \"last-delivered-id\",\"1580474520477-0\"]]\n"
"> xsetid S G 0\n"
"'OK'\n"
"> xinfo groups S\n"
"[[\"name\",\"G\",\"consumers\",0,\"pending\",0,\"last-delivered-id\",\"0\"]]\n"},
{ &xtra[810], XTRA_DESCR,
"Set the next *id* of a group associated with a stream.\n"},
{ NULL, XTRA_RETURN,
"*OK* if success, error when key or group doesn't exist.\n"},
};
#endif

}
}
#endif
