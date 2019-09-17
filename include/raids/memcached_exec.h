#ifndef __rai_raids__memcached_exec_h__
#define __rai_raids__memcached_exec_h__

#include <raids/ev_key.h>
#include <raids/stream_buf.h>

namespace rai {
namespace ds {

enum MemcachedStatus {
  MEMCACHED_OK = 0,
  MEMCACHED_MSG_PARTIAL,  /* need more read */
  MEMCACHED_EMPTY,        /* no command, empty lline */
  MEMCACHED_SETUP_OK,     /* key setup */
  MEMCACHED_SUCCESS  = EK_SUCCESS,  /* all key status good */
  MEMCACHED_DEPENDS  = EK_DEPENDS,  /* key depends on another */
  MEMCACHED_CONTINUE = EK_CONTINUE, /* not all keys complete */
  MEMCACHED_QUIT,         /* quit command */
  MEMCACHED_VERSION,      /* version command */
  MEMCACHED_ALLOC_FAIL,   /* allocation returned NULL */
  MEMCACHED_BAD_CMD,      /* bad command, or unknown cmd */
  MEMCACHED_BAD_ARGS,     /* bad argument parse */
  MEMCACHED_INT_OVERFLOW, /* int overflows */
  MEMCACHED_BAD_INT,      /* integer is malformed */
  MEMCACHED_BAD_INCR,     /* cannot increment or decrement non-numeric */
  MEMCACHED_ERR_KV,       /* kstatus != ok */
  MEMCACHED_BAD_TYPE,     /* key exists, but is not a string type */
  MEMCACHED_NOT_IMPL,     /* cmd exists, but not wired */
  MEMCACHED_BAD_PAD,      /* sentinal not set */
  MEMCACHED_BAD_BIN_ARGS, /* binary message format error */
  MEMCACHED_BAD_BIN_CMD,  /* binary message command error */
  MEMCACHED_BAD_RESULT    /* parsing result status failed */
};

inline static bool memcached_status_success( int status ) {
  return status <= MEMCACHED_SUCCESS; /* the OK and SEND_xxx status */
}

inline static bool memcached_status_fail( int status ) {
  return status > MEMCACHED_SUCCESS;  /* the bad status */
}

const char * memcached_status_string( MemcachedStatus status );
const char * memcached_status_description( MemcachedStatus status );

enum MemcachedCmd {
  MC_NONE = 0,
  /* storage */
  MC_SET,     /* SET key flags ttl msglen [noreply]<CR><NL>msg<CR><NL>
                 STORED<CR><NL> */
  MC_ADD,     /* ADD ... (store only if key doesn't exist) */
  MC_REPLACE, /* REPLACE ... (store only if key exists) */
  MC_APPEND,  /* APPEND ... (append to key if it exists) */
  MC_PREPEND, /* PREPEND ... (prepend to key if it exists) */
  MC_CAS,     /* CAS key flags ttl msglen cas-unique-id [noreply] ... */
  /* retrieval */
  MC_GET,     /* GET key [key2] ...
                 VALUE key flags msglen<CR><NL>
                 msg<CR><NL>
                 [VALUE key2 flags msglen<CR><NL>
                 msg<CR><NL>]
                 END<CR><NL> */
  MC_GETS,    /* GETS key [key2] ...
                 VALUE key flags msglen cas-unique-id ... */
  /* removal */
  MC_DELETE,  /* DELETE key [noreply]<CR><NL>
                 DELETED<CR><NL> or
                 NOT_FOUND<CR><NL> */
  /* math */
  MC_INCR,    /* INCR key value [noreply]<CR><NL> */
  MC_DECR,    /* DECR ... (key must exist and be unsigned integer >= value)
                 value<CR><NL> or
                 NOT_FOUND<CR><NL> */
  /* touch */
  MC_TOUCH,   /* TOUCH key ttl [noreply]
                 TOUCHED<CR><NL> or
                 NOT_FOUND<CR><NL> */
  MC_GAT,     /* GAT ttl key [key2] ... */
  MC_GATS,    /* GATS ttl key [key2] ... */
  /* cache */
  MC_SLABS,   /* SLABS source dest<CR><NL>
                 SLABS automove 0|1<CR><NL> */
  MC_LRU,     /* LRU tune|mode|temp_ttl options<CR><NL> */
  MC_LRU_CRAWLER, /* LRU_CRAWLER enable|disable<CR><NL>
                     LRU_CRAWLER sleep micros<CR><NL>
                     LRU_CRAWLER tocrawl<CR><NL>
                     LRU_CRAWLER metadump classid|all<CR><NL> */
  MC_WATCH,   /* WATCH fetchers|mutations|evictions */
  MC_STATS,   /* STATS [args] */
  MC_FLUSH_ALL, /* FLUSH_ALL [num] */
  MC_CACHE_MEMLIMIT, /* CACHE_MEMLIMIT num */
  MC_VERSION, /* VERSION */
  MC_QUIT,    /* QUIT (close connection) */
  MC_NO_OP,   /* NO OP keep alive */
  MC_VERBOSITY, /* VERIBOSITY(25) (increase logging verb) */

  MC_CMD_MASK = 31, /* mask commands above */
  MC_QUIET    = 32, /* options for binary commands */
  MC_KEY      = 64,
  MC_BINARY   = 128
};

enum MemcachedResult {
  MR_NONE = 0,
  MR_END,          /* terminates a get/gets */
  MR_DELETED,      /* if delete sucess (otherwise not_found) */
  MR_STORED,       /* if set/add/repl/.. success (otherwise not_stored) */
  MR_VALUE,        /* if get/gets success */
  MR_INT,          /* if incr/decr success (otherwise NOT_FOUND) */
  MR_TOUCHED,      /* if touch success (otherwise NOT_FOUND) */
  MR_NOT_FOUND,    /* if some op did not find the key */
  MR_NOT_STORED,   /* if a storage op did not find the key */
  MR_EXISTS,       /* if a cas op fails */
  MR_ERROR,        /* a command parse error */
  MR_CLIENT_ERROR, /* a protocol error */
  MR_SERVER_ERROR, /* a server execution error */
  MR_BUSY,         /* slabs, try later */
  MR_BADCLASS,     /* slabs, class id error */
  MR_NOSPARE,      /* slabs, no spare page */
  MR_NOTFULL,      /* slabs, dest class must be full to move it */
  MR_UNSAFE,       /* slabs, source cant move page right now */
  MR_SAME,         /* slabs, source dest must be different */
  MR_OK,           /* lru, others, ok */
  MR_STAT,         /* stats, some stat name/value*/
  MR_VERSION       /* version # */
};

#define B( e ) ( (uint64_t) 1 << ( e ) )
static const uint64_t
  read_only = B( MC_GET )   | B( MC_GETS ),

  mutator   = B( MC_SET )   | B( MC_ADD )   | B( MC_REPLACE ) | B( MC_APPEND ) |
            B( MC_PREPEND ) | B( MC_CAS )   | B( MC_DELETE )  | B( MC_INCR ) |
              B( MC_DECR )  | B( MC_TOUCH ) | B( MC_GAT )     | B( MC_GATS );

static inline bool test_read_only( uint8_t c ) {
  return ( B( c & MC_CMD_MASK ) & read_only ) != 0;
}
static inline bool test_mutator( uint8_t c ) {
  return ( B( c & MC_CMD_MASK ) & mutator ) != 0;
}
#undef B

const char *memcached_cmd_string( uint8_t cmd );
const char *memcached_res_string( uint8_t res );

/* presumes little endian, 0xdf masks out 0x20 for toupper() */
#define C4_KW( c1, c2, c3, c4 ) ( ( (uint32_t) ( c4 & 0xdf ) << 24 ) | \
                                  ( (uint32_t) ( c3 & 0xdf ) << 16 ) | \
                                  ( (uint32_t) ( c2 & 0xdf ) << 8 ) | \
                                  ( (uint32_t) ( c1 & 0xdf ) ) )
#define MC_KW_SET             C4_KW( 'S', 'E', 'T', ' ' )
#define MC_KW_ADD             C4_KW( 'A', 'D', 'D', ' ' )
#define MC_KW_APPEND          C4_KW( 'A', 'P', 'P', 'E' )
#define MC_KW_REPLACE         C4_KW( 'R', 'E', 'P', 'L' )
#define MC_KW_PREPEND         C4_KW( 'P', 'R', 'E', 'P' )
#define MC_KW_CAS             C4_KW( 'C', 'A', 'S', ' ' )
#define MC_KW_GET             C4_KW( 'G', 'E', 'T', ' ' )
#define MC_KW_GETS            C4_KW( 'G', 'E', 'T', 'S' )
#define MC_KW_GAT             C4_KW( 'G', 'A', 'T', ' ' )
#define MC_KW_GATS            C4_KW( 'G', 'A', 'T', 'S' )
#define MC_KW_DELETE          C4_KW( 'D', 'E', 'L', 'E' )
#define MC_KW_DECR            C4_KW( 'D', 'E', 'C', 'R' )
#define MC_KW_INCR            C4_KW( 'I', 'N', 'C', 'R' )
#define MC_KW_TOUCH           C4_KW( 'T', 'O', 'U', 'C' )
#define MC_KW_SLABS           C4_KW( 'S', 'L', 'A', 'B' )
#define MC_KW_LRU             C4_KW( 'L', 'R', 'U', ' ' )
#define MC_KW_LRU_CRAWLER     C4_KW( 'L', 'R', 'U', '_' )
#define MC_KW_WATCH           C4_KW( 'W', 'A', 'T', 'C' )
#define MC_KW_STATS           C4_KW( 'S', 'T', 'A', 'T' )
#define MC_KW_FLUSH_ALL       C4_KW( 'F', 'L', 'U', 'S' )
#define MC_KW_CACHE_MEMLIMIT  C4_KW( 'C', 'A', 'C', 'H' )
#define MC_KW_VERSION         C4_KW( 'V', 'E', 'R', 'S' )
#define MC_KW_QUIT            C4_KW( 'Q', 'U', 'I', 'T' )
#define MC_KW_NO_OP           C4_KW( 'N', 'O', '_', 'O' )

/* missing: NOT_STORED, INT */
#define MR_KW_OK              C4_KW( 'O', 'K',  0,   0  )
#define MR_KW_END             C4_KW( 'E', 'N', 'D',  0  )
#define MR_KW_DELETED         C4_KW( 'D', 'E', 'L', 'E' )
#define MR_KW_STORED          C4_KW( 'S', 'T', 'O', 'R' )
#define MR_KW_VALUE           C4_KW( 'V', 'A', 'L', 'U' )
#define MR_KW_TOUCHED         C4_KW( 'T', 'O', 'U', 'C' )
#define MR_KW_NOT_FOUND       C4_KW( 'N', 'O', 'T', '_' )
#define MR_KW_EXISTS          C4_KW( 'E', 'X', 'I', 'S' )
#define MR_KW_ERROR           C4_KW( 'E', 'R', 'R', 'O' )
#define MR_KW_CLIENT_ERROR    C4_KW( 'C', 'L', 'I', 'E' )
#define MR_KW_SERVER_ERROR    C4_KW( 'S', 'E', 'R', 'V' )
#define MR_KW_BUSY            C4_KW( 'B', 'U', 'S', 'Y' )
#define MR_KW_BADCLASS        C4_KW( 'B', 'A', 'D', 'C' )
#define MR_KW_NOSPARE         C4_KW( 'N', 'O', 'S', 'P' )
#define MR_KW_NOTFULL         C4_KW( 'N', 'O', 'T', 'F' )
#define MR_KW_UNSAFE          C4_KW( 'U', 'N', 'S', 'A' )
#define MR_KW_SAME            C4_KW( 'S', 'A', 'M', 'E' )
#define MR_KW_STAT            C4_KW( 'S', 'T', 'A', 'T' )
#define MR_KW_VERSION         C4_KW( 'V', 'E', 'R', 'S' )


/* Request (magic=0x80) / Response (0x81) header
 * (https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped):
          0       |       1       |       2       |       3       |
  +---------------+---------------+---------------+---------------+
0 | Magic         | Opcode        | Key length                    |
  +---------------+---------------+---------------+---------------+
4 | Extras length | Data type     | Resp. Status / Req. VBucket   |
  +---------------+---------------+---------------+---------------+
8 | Total body length                                             |
  +---------------+---------------+---------------+---------------+
12| Opaque                                                        |
  +---------------+---------------+---------------+---------------+
16| CAS  (64 bits)                                                |
  |                                                               |
  +---------------+---------------+---------------+---------------+
24*/
struct MemcachedBinHdr { /* from "Packet Structure" */
  uint8_t  magic,     /* 0x80 = request, 0x81 = response */
           opcode;    /* MemcachedBinOpcode below */
  uint16_t keylen;    /* len of key */
  uint8_t  extralen,  /* len of flags + ttl, if any */
           data_type; /* always 0 */
  uint16_t status;    /* MemcacheBinStatus below */
  uint32_t datalen,   /* length of data, extra + key + msg */
           opaque;    /* ?? */
  uint64_t cas;       /* cas, non-zero when used */
};

enum MemcachedBinStatus { /* from "Response Status" */
  MS_OK              = 0, /* status field in binary header */
  MS_NOT_FOUND       = 1, /* "Not found" */
  MS_EXISTS          = 2, /* "Data exists for key." */
  MS_VALUE_TOO_BIG   = 3, /* "Too large." */
  MS_BAD_ARGS        = 4, /* "Invalid arguments" */
  MS_NOT_STORED      = 5, /* "Not stored." */
  MS_BAD_INCR_VALUE  = 6, /* "Non-numeric server-side value for incr or decr" */
  MS_BAD_VBUCKET     = 7, /* "Bad vbucket" */
  MS_AUTH_ERROR      = 8, /* "Auth failure." */
  MS_AUTH_CONTINUE   = 9, /* "Continue" */
  MS_UNKNOWN_COMMAND = 129, /* "Unknown command" */
  MS_ALLOC_FAIL      = 130, /* "Out of memory" */
  MS_NOT_SUPPORTED   = 131, /* "Not supported" */
  MS_INTERNAL_ERROR  = 132, /* "Internal error" */
  MS_BUSY            = 133, /* "Busy" */
  MS_TEMP_FAIL       = 134  /* "Temporary failure" */
};

enum MemcachedBinOpcode { /* from "Command Opcodes" */
  OP_GET        = 0x00,
  OP_SET        = 0x01,
  OP_ADD        = 0x02,
  OP_REPLACE    = 0x03,
  OP_DELETE     = 0x04,
  OP_INCREMENT  = 0x05,
  OP_DECREMENT  = 0x06,
  OP_QUIT       = 0x07,
  OP_FLUSH      = 0x08,
  OP_GETQ       = 0x09,
  OP_NO_OP      = 0x0a,
  OP_VERSION    = 0x0b,
  OP_GETK       = 0x0c,
  OP_GETKQ      = 0x0d,
  OP_APPEND     = 0x0e,
  OP_PREPEND    = 0x0f,
  OP_STAT       = 0x10,
  OP_SETQ       = 0x11,
  OP_ADDQ       = 0x12,
  OP_REPLACEQ   = 0x13,
  OP_DELETEQ    = 0x14,
  OP_INCREMENTQ = 0x15,
  OP_DECREMENTQ = 0x16,
  OP_QUITQ      = 0x17,
  OP_FLUSHQ     = 0x18,
  OP_APPENDQ    = 0x19,
  OP_PREPENDQ   = 0x1a,
  OP_VERBOSITY  = 0x1b,
  OP_TOUCH      = 0x1c,
  OP_GAT        = 0x1d,
  OP_GATQ       = 0x1e,

  OP_SASL_LIST  = 0x20,
  OP_SASL_AUTH  = 0x21,
  OP_SASL_STEP  = 0x22,

  OP_RGET       = 0x30, /* not implemented */
  OP_RSET       = 0x31,
  OP_RSETQ      = 0x32,
  OP_RAPPEND    = 0x33,
  OP_RAPPENDQ   = 0x34,
  OP_RPREPEND   = 0x35,
  OP_RPREPENDQ  = 0x36,
  OP_RDELETE    = 0x37,
  OP_RDELETEQ   = 0x38,
  OP_RINCR      = 0x39,
  OP_RINCRQ     = 0x3a,
  OP_RDECR      = 0x3b,
  OP_RDECRQ     = 0x3c,

  OP_SET_VBUCKET          = 0x3d, /* proposed ver 1.6 */
  OP_GET_VBUCKET          = 0x3e,
  OP_DEL_VBUCKET          = 0x3f,
  OP_TAP_CONNECT          = 0x40,
  OP_TAP_MUTATION         = 0x41,
  OP_TAP_DELETE           = 0x42,
  OP_TAP_FLUSH            = 0x43,
  OP_TAP_OPAQUE           = 0x44,
  OP_TAP_VBUCKET_SET      = 0x45,
  OP_TAP_CHECKPOINT_START = 0x46,
  OP_TAP_CHECKPOINT_END   = 0x47
};

struct MemcachedArgs { /* argv[] starts after command name */
  char * str;
  size_t len;
};

struct MemcachedMsg { /* both ASCII and binary */
  MemcachedArgs * args;    /* not including cmd string */
  uint32_t        argcnt,  /* args[] size */
                  keycnt,  /* how many keys */
                  first,   /* args[ first ] is the first key */
                  flags;   /* flags when storing a key */
  uint64_t        ttl,     /* expire time */
                  cas,     /* compare and swap val returned with GETS */
                  msglen,  /* msg length of the data attached for storage */
                  inc;     /* increment / decrement amount */
  char          * msg;     /* message data, length msglen for storage */
  MemcachedArgs   xarg;    /* one arg buffer */
  uint8_t         cmd,     /* command enum MemcachedCmd w/flags >= 32 */
                  opcode,  /* binary opcode, if cmd & MC_BINARY is set */
                  res,     /* binary parse error, if nonzero */
                  pad;     /* mem marker */
  uint32_t        opaque;  /* binary opaque, passed through to client */
  uint64_t        ini;     /* binary init for incr / decr */

  MemcachedCmd command( void ) const {
    return (MemcachedCmd) ( this->cmd & MC_CMD_MASK );
  }
  bool is_quiet( void ) const {
    return ( this->cmd & MC_QUIET ) != 0;
  }
  bool is_binary( void ) const {
    return ( this->cmd & MC_BINARY ) != 0;
  }
  bool wants_key( void ) const {
    return ( this->cmd & MC_KEY ) != 0;
  }
  void zero( uint8_t c ) {
    ::memset( this, 0, sizeof( *this ) );
    this->cmd = c;
  }
  bool match_arg( const char *name,  size_t namelen ) const {
    if ( this->argcnt < 1 )
      return false;
    if ( namelen != this->args[ 0 ].len )
      return false;
    if ( ::strncasecmp( name, this->args[ 0 ].str, namelen ) != 0 )
      return false;
    return true;
  }
  void print( void ); /* use only after unpack() successful */
  /* parse a memcached command and initialize this structure */
  MemcachedStatus unpack( void *buf,  size_t &buflen,  kv::ScratchMem &wrk );
  MemcachedStatus parse_store( void );
  MemcachedStatus parse_bin_store( MemcachedBinHdr &b,  char *ptr,
                                   size_t &buflen );
  MemcachedStatus parse_bin_pend( MemcachedBinHdr &b,  char *ptr,
                                  size_t &buflen );
  MemcachedStatus parse_cas( void );
  MemcachedStatus parse_retr( void );
  MemcachedStatus parse_bin_retr( MemcachedBinHdr &b,  char *ptr,
                                  size_t &buflen );
  MemcachedStatus parse_gat( void );
  MemcachedStatus parse_bin_touch( MemcachedBinHdr &b,  char *ptr,
                                   size_t &buflen );
  MemcachedStatus parse_del( void );
  MemcachedStatus parse_incr( void );
  MemcachedStatus parse_bin_incr( MemcachedBinHdr &b,  char *ptr,
                                 size_t &buflen );
  MemcachedStatus parse_touch( void );
  MemcachedStatus parse_bin_op( MemcachedBinHdr &b,  char *ptr,
                                size_t &buflen,  size_t extra_sz );
};

struct MemcachedRes {
  MemcachedArgs * args;    /* not including cmd string */
  uint32_t        argcnt,  /* args[] size */
                  flags;
  uint64_t        ival,
                  cas,
                  msglen;
  char          * msg,
                * key;
  MemcachedArgs   xarg;    /* one arg buffer */
  uint8_t         res; /* MemcachedResult */
  bool            is_err;
  uint16_t        keylen;

  void zero( uint8_t r ) {
    ::memset( this, 0, sizeof( *this ) );
    this->res = r;
  }
  void print( void );
  MemcachedStatus unpack( void *buf,  size_t &buflen,  kv::ScratchMem &wrk );
  MemcachedStatus parse_value_result( void );
};

struct MemcachedStats {
  char     interface[ 44 ];
  uint16_t tcpport, udpport;
  uint32_t max_connections, curr_connections, total_connections, conn_structs;
  uint64_t boot_time,
           get_cnt, set_cnt, flush_cnt, touch_cnt,
           get_hit, get_miss, get_expired, get_flushed,
           delete_miss, delete_hit,
           incr_miss, incr_hit,
           decr_miss, decr_hit,
           cas_miss, cas_hit, cas_badval,
           touch_hit, touch_miss,
           auth_cmds, auth_errors,
           bytes_read, bytes_written;
};

struct MemcachedExec {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  uint64_t seed,   seed2;     /* kv map hash seeds, different for each db */
  kv::KeyCtx       kctx;      /* key context used for every key in command */
  kv::WorkAllocT< 1024 > wrk; /* kv work buffer, reset before each key lookup */
  StreamBuf      & strm;      /* output buffer, result of command execution */
  MemcachedMsg   * msg;       /* current command msg */
  EvKeyCtx       * key,       /* currently executing key */
                ** keys;      /* all of the keys in command */
  uint32_t         key_cnt,   /* total keys[] size */
                   key_done;  /* number of keys processed */
  MemcachedStats & stat;

  MemcachedExec( kv::HashTab &map,  uint32_t ctx_id,  StreamBuf &s,
                 MemcachedStats &st ) :
      kctx( map, ctx_id, NULL ), strm( s ), msg( 0 ),
      key( 0 ), keys( 0 ), key_cnt( 0 ), key_done( 0 ), stat( st ) {
    this->kctx.ht.hdr.get_hash_seed( this->kctx.db_num, this->seed,
                                     this->seed2 );
    this->kctx.set( kv::KEYCTX_NO_COPY_ON_READ );
  }
  MemcachedStatus unpack( void *buf,  size_t &buflen );
  MemcachedStatus exec_key_setup( EvSocket *own,  EvPrefetchQueue *q,
                                  EvKeyCtx *&ctx,  uint32_t n );
  MemcachedStatus exec( EvSocket *svc,  EvPrefetchQueue *q );
  /* compute the hash and prefetch the ht[] location */
  void exec_key_prefetch( EvKeyCtx &ctx ) {
    this->key = ctx.prefetch( this->kctx );
  }
  kv::KeyStatus exec_key_fetch( EvKeyCtx &ctx,  bool force_read );
  MemcachedStatus exec_key_continue( EvKeyCtx &ctx );
  /* fetch key for write and check type matches or is not set */
  kv::KeyStatus get_key_write( EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, false );
    if ( status == KEY_OK && ctx.type != type ) {
      if ( ctx.type == 0 ) {
        ctx.is_new = true;
        return KEY_IS_NEW;
      }
      return KEY_NO_VALUE;
    }
    return status;
  }
  /* fetch key for read and check type matches or is not set */
  kv::KeyStatus get_key_read( EvKeyCtx &ctx,  uint8_t type ) {
    kv::KeyStatus status = this->exec_key_fetch( ctx, true );
    if ( status == KEY_OK && ctx.type != type )
      return ( ctx.type == 0 ) ? KEY_NOT_FOUND : KEY_NO_VALUE;
    return status;
  }
  void multi_get_send( void );
  size_t send_value( EvKeyCtx &ctx,  const void *data,  size_t size );
  size_t send_bin_value( EvKeyCtx &ctx,  const void *data,  size_t size );
  bool save_value( EvKeyCtx &ctx,  const void *data,  size_t size );
  MemcachedStatus exec_store( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_store( EvKeyCtx &ctx );
  MemcachedStatus exec_retr( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_retr( EvKeyCtx &ctx );
  MemcachedStatus exec_retr_touch( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_retr_touch( EvKeyCtx &ctx );
  MemcachedStatus exec_del( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_del( EvKeyCtx &ctx );
  MemcachedStatus exec_touch( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_touch( EvKeyCtx &ctx );
  MemcachedStatus exec_incr( EvKeyCtx &ctx );
  MemcachedStatus exec_bin_incr( EvKeyCtx &ctx );

  void exec_run_to_completion( void );
  void send_err( int status,  kv::KeyStatus kstatus = KEY_OK );
  size_t send_string( const void *s );
  size_t send_bin_status( uint16_t status, const void *s = NULL,
                          size_t slen = 0 );
  size_t send_bin_status_key( uint16_t status, EvKeyCtx &ctx );
  size_t send_string( const void *s,  size_t slen );
  size_t send_err_kv( kv::KeyStatus kstatus );
  void put_stats( void );
  void put_stats_settings( void );
  void put_stats_items( void );
  void put_stats_sizes( void );
  void put_stats_slabs( void );
  void put_stats_conns( void );
  void release( void ) {
    this->wrk.release_all();
  }
  bool do_slabs( void );
  bool do_lru( void );
  bool do_lru_crawler( void );
  bool do_watch( void );
  bool do_flush_all( void );
  bool do_memlimit( void );
  void do_no_op( void );
};

}
}
#endif
