#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raids/redis_cmd_db.h>
#include "redis_cmd_db.cpp"

/* offset +1 from enum (NO_CATG=0), so category can be accessed by:
 * RedisCatg redis_catg = STREAM_CATG;
 * const char *name = cmd_category[ (int) redis_catg - 1 ].sub_type;
 */
static const struct {
  int cnt;
  const char *sub_type;
}
cmd_category[] =
{
  { CLUSTER_CNT,     "CLUSTER" },
  { CONNECTION_CNT,  "CONNECTION" },
  { GEO_CNT,         "GEO" },
  { HASH_CNT,        "HASH" },
  { HYPERLOGLOG_CNT, "HYPERLOGLOG" },
  { KEY_CNT,         "KEY" },
  { LIST_CNT,        "LIST" },
  { PUBSUB_CNT,      "PUBSUB" },
  { SCRIPT_CNT,      "SCRIPT" },
  { SERVER_CNT,      "SERVER" },
  { SET_CNT,         "SET" },
  { SORTED_SET_CNT,  "SORTED_SET" },
  { STRING_CNT,      "STRING" },
  { TRANSACTION_CNT, "TRANSACTION" },
  { STREAM_CNT,      "STREAM" },
  { 0,               NULL },
};

static const size_t cmd_category_cnt = sizeof( cmd_category ) /
                                       sizeof( cmd_category[ 0 ] ) - 1;

/* generate redis_cmd.h unless included in another c++ file with this
 * defined: */
#ifndef NO_REDIS_CMD_GENERATE

#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raids/redis_msg.h>
#include <ctype.h>

using namespace rai;
using namespace ds;

static int
get_cmd_flag( const char *s )
{
  for ( int i = 0; i < (int) cmd_flag_cnt; i++ ) {
    if ( *(int *) (void *) s == *(int *) (void *) cmd_flag[ i ] )
      return 1 << i;
  }
  return 0;
}

static char *
get_cmd_upper( int i )
{
  static char buf[ 32 ];
  int j = 0;
  for ( const char *s = cmd_flag[ i ]; ; s++ ) {
    if ( (buf[ j++ ] = toupper( *s )) == '\0' )
      return buf;
  }
}

static void
gen_enums( void )
{
  size_t i = 0, j = 0, k, maxlen = 18, len, catsum;
  uint32_t cnt = 0;
  const char *cat, *cmd, *comma;

  printf( "enum RedisCatg {\n  NO_CATG%*s =  0,\n", (int) ( maxlen - 3 ), "" );
  comma = ",";
  for ( i = 0, cnt = 1; i < cmd_category_cnt; cnt++ ) {
    cat = cmd_category[ i ].sub_type;
    if ( ++i == cmd_category_cnt )
      comma = "";
    len = ::strlen( cat );
    printf( "  %s_CATG%*s = %2u%s\n", cat, (int) ( maxlen - len - 1 ), "",
            cnt, comma );
  }
  printf( "};\n\n" );
  printf( "enum RedisCmd {\n  NO_CMD%*s =   0,\n", (int) ( maxlen - 2 ), "" );
  comma = ",";
  printf( "  /* %s */\n", cmd_category[ 0 ].sub_type );
  j = cmd_category[ k=0 ].cnt;
  for ( i = 1, cnt = 1; i < cmd_db_cnt; cnt++ ) {
    cmd = cmd_db[ i ].name;
    len = ::strlen( cmd );
    if ( ++i == cmd_db_cnt )
      comma = "";
    printf( "  %.*s_CMD%*s = %3u%s\n",
            (int) len, cmd, (int) ( maxlen - len ), "", cnt, comma );
    if ( cnt >= j ) {
      if ( ++k < cmd_category_cnt ) {
        printf( "  /* %s */\n", cmd_category[ k ].sub_type );
        j += cmd_category[ k ].cnt;
      }
    }
  }
  printf( "};\n\n"
          "static const size_t REDIS_CATG_COUNT = %d,\n"
          "                    REDIS_CMD_COUNT  = %d;\n\n",
          (int) cmd_category_cnt + 1, (int) cmd_db_cnt );

  printf( "static inline void\n"
          "get_cmd_arity( RedisCmd cmd,  int16_t &arity,  int16_t &first,  "
                         "int16_t &last,  int16_t &step ) {\n"
          "  /* Arity of commands indexed by cmd */\n"
          "  static const uint16_t redis_cmd_arity[] = {\n    0" );
  for ( i = 1; i < cmd_db_cnt; i++ ) {
    kv::WorkAllocT<256> wrk;
    RedisMsg m;
    union {
      struct {
        int arity : 4,
            first : 4,
            last  : 4,
            step  : 4;
      } b;
      uint16_t val;
    } u;
    u.val = 0;
    if ( m.unpack_json( cmd_db[ i ].attr, wrk ) == REDIS_MSG_OK ) {
      if ( m.type == RedisMsg::BULK_ARRAY && m.len > 1 ) {
        if ( m.array[ 1 ].type == RedisMsg::INTEGER_VALUE )
          u.b.arity = (int) m.array[ 1 ].ival;
        if ( m.len > 3 && m.array[ 3 ].type == RedisMsg::INTEGER_VALUE )
          u.b.first = (int) m.array[ 3 ].ival;
        if ( m.len > 4 && m.array[ 4 ].type == RedisMsg::INTEGER_VALUE )
          u.b.last = (int) m.array[ 4 ].ival;
        if ( m.len > 5 && m.array[ 5 ].type == RedisMsg::INTEGER_VALUE )
          u.b.step = (int) m.array[ 5 ].ival;
      }
    }
    printf( ",0x%x", u.val );
    wrk.reset();
  }
  printf( "};\n"
          "  union {\n"
          "    struct {\n"
          "      int arity : 4, first : 4, last : 4, step : 4;\n"
          "    } b;\n"
          "    uint16_t val;\n"
          "  } u;\n"
          "  u.val = redis_cmd_arity[ cmd ];\n"
          "  arity = u.b.arity; first = u.b.first;\n"
          "  last  = u.b.last;  step  = u.b.step;\n"
          "}\n\n" );

  printf( "/* Flags enum:  used to test flags below ( 1 << CMD_FAST_FLAG )*/\n"
          "enum RedisCmdFlag {\n" );
  for ( i = 0; i < cmd_flag_cnt; i++ ) {
    printf( "  CMD_%s_FLAG%*s= %2d,\n", get_cmd_upper( i ),
            (int) ( 16 - ::strlen( cmd_flag[ i ] ) ), "", (int) i );
  }
  printf( "  CMD_MAX_FLAG%*s= %2d\n", 16 - 3, "", (int) cmd_flag_cnt );
  printf( "};\n\n" );

  printf( "static inline uint16_t\n"
          "get_cmd_flag_mask( RedisCmd cmd ) {\n"
          "  /* Bit mask of flags indexed by cmd */\n"
          "  static const uint16_t redis_cmd_flags[] = {\n    0" );
  for ( i = 1; i < cmd_db_cnt; i++ ) {
    kv::WorkAllocT<256> wrk;
    RedisMsg m;
    int flags = 0;
    if ( m.unpack_json( cmd_db[ i ].attr, wrk ) == REDIS_MSG_OK ) {
      if ( m.type == RedisMsg::BULK_ARRAY && m.len > 2 )
        if ( m.array[ 2 ].type == RedisMsg::BULK_ARRAY )
          for ( j = 0; j < (size_t) m.array[ 2 ].len; j++ )
            if ( m.array[ 2 ].array[ j ].type == RedisMsg::SIMPLE_STRING )
              flags |= get_cmd_flag( m.array[ 2 ].array[ j ].strval );
    }
    printf( ",0x%x", flags );
    wrk.reset();
  }
  printf( "};\n"
          "  return redis_cmd_flags[ cmd ];\n"
          "}\n\n" );
  printf( "static inline bool\n"
          "test_cmd_flag( RedisCmd cmd,  RedisCmdFlag fl ) {\n"
          "  return ( get_cmd_flag_mask( cmd ) & ( 1U << (int) fl ) ) != 0;\n"
          "}\n\n" );
  printf( "static inline uint16_t\n"
          "test_cmd_mask( uint16_t mask,  RedisCmdFlag fl ) {\n"
          "  return mask & ( 1U << (int) fl );\n"
          "}\n\n" );

  /* this presumes categories fits in 4 bits (0->15) */
  uint32_t mask[ ( cmd_db_cnt * 4 + 31 ) / 32 ];
  ::memset( mask, 0, sizeof( mask ) );
  catsum = 1;
  for ( i = 0; i < cmd_category_cnt; i++ ) {
    j = catsum;
    catsum += cmd_category[ i ].cnt;
    for ( ; j < catsum; j++ )
      mask[ j >> 3 ] |= ( i + 1 ) << ( 4 * ( j & 7 ) );
  }

  printf( "static inline RedisCatg\nget_cmd_category( RedisCmd cmd ) {\n"
          "  static const uint32_t catg[] = {\n"
          "    0x%08xU", mask[ 0 ] );
  for ( i = 1; i < sizeof( mask ) / sizeof( mask[ 0 ] ); i++ ) {
    printf( ",0x%08xU", mask[ i ] );
  }
  printf( " };\n"
          "  uint32_t x = (uint32_t) cmd;\n"
          "  x = ( catg[ x >> 3 ] >> ( 4 * ( x & 7 ) ) ) & 0xf;\n"
          "  return (RedisCatg) x;\n"
          "}\n\n" );
}

/* try random seeds and incr ht sizes until no collisions */
static void
gen_perfect_hash( void )
{
  kv::rand::xoroshiro128plus rand;
  uint64_t x = 0;
  uint32_t ht[ 1024 ];
  size_t   i;
  uint32_t r = 0x36fbdccd;
  const char *cmd;
  int k = 0;

  rand.init();
  goto try_first;
  for (;;) {
    if ( ( k & 1 ) == 0 ) {
      x = rand.next();
      r = (uint32_t) x;
    }
    else {
      r = x >> 32;
    }
  try_first:;
    ::memset( ht, 0, sizeof( ht ) );
    for ( i = 1; i < cmd_db_cnt; i++ ) {
      const char *cmd = cmd_db[ i ].name;
      int len = ::strlen( cmd );

      uint32_t h = kv_crc_c( cmd, len, r );
      uint32_t *p = &ht[ h % 1024 ];
      if ( h == 0 || *p != 0 )
        goto try_next;
      *p = h;
    }
    goto gen_hash_tab;
  try_next:;
    k++;
  }
gen_hash_tab:;
  printf( "/* Generated ht[] is a perfect hash, but does not strcmp() cmd */\n"
          "static inline RedisCmd\n"
          "get_redis_cmd( const char *cmd,  size_t len ) {\n"
          "  static const uint8_t ht[] = {\n    " );
  ::memset( ht, 0, sizeof( ht ) );
  for ( i = 1; i < cmd_db_cnt; i++ ) {
    cmd = cmd_db[ i ].name;
    int len = ::strlen( cmd );

    uint32_t h = kv_crc_c( cmd, len, r );
    ht[ h % 1024 ] = i;
  }
  printf( "%u", ht[ 0 ] );
  for ( i = 1; i < 1024; i++ ) {
    printf( ",%u", ht[ i ] );
  }
  printf( "};\n"
          "  static const uint32_t hashes[] = {\n    0" );
  for ( i = 1; i < cmd_db_cnt; i++ ) {
    cmd = cmd_db[ i ].name;
    uint32_t h = kv_crc_c( cmd, ::strlen( cmd ), r );
    printf( ",0x%x", h );
  }
  printf( "};\n"
         "  uint32_t k = kv_crc_c( cmd, len, 0x%x );\n"
         "  uint8_t  c = ht[ k %% %u ];\n"
         "  return hashes[ c ] == k ? (RedisCmd) c : NO_CMD;\n"
         "}\n\n", r, 1024 );
}

int
main( int, char ** )
{
  printf( "#ifndef __rai_raids__redis_cmd_h__\n"
          "#define __rai_raids__redis_cmd_h__\n\n"
          "#include <raikv/key_hash.h>\n\n"
          "namespace rai {\n"
          "namespace ds {\n\n" );
  gen_enums();
  gen_perfect_hash();
  printf( "}\n"
          "}\n"
          "#endif\n" );
  return 0;
}
#endif

