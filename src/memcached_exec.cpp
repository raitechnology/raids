#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/util.h>
#include <raids/memcached_exec.h>
#include <raids/int_str.h>
#include <raimd/decimal.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

static const char   mc_end[]          = "END";
static const size_t mc_end_len        = sizeof( mc_end ) - 1;
static const char   mc_del[]          = "DELETED";
static const size_t mc_del_len        = sizeof( mc_del ) - 1;
static const char   mc_stored[]       = "STORED";
static const size_t mc_stored_len     = sizeof( mc_stored ) - 1;
static const char   mc_value[]        = "VALUE";
static const size_t mc_value_len      = sizeof( mc_value ) - 1;
static const char   mc_touch[]        = "TOUCHED";
static const size_t mc_touch_len      = sizeof( mc_touch ) - 1;
static const char   mc_not_found[]    = "NOT_FOUND";
static const size_t mc_not_found_len  = sizeof( mc_not_found ) - 1;
static const char   mc_not_stored[]   = "NOT_STORED";
static const size_t mc_not_stored_len = sizeof( mc_not_stored ) - 1;
static const char   mc_exists[]       = "EXISTS";
static const size_t mc_exists_len     = sizeof( mc_exists ) - 1;
static const char   mc_ok[]           = "OK";
static const size_t mc_ok_len         = sizeof( mc_ok ) - 1;
static const char   mc_error[]        = "ERROR";
static const size_t mc_error_len      = sizeof( mc_error ) - 1;

enum {
  /* add = must not exist, replace = must exist */
  MC_MUST_NOT_EXIST = 1,
  MC_MUST_EXIST     = 2,
  MC_DO_PEND        = 4,
  MC_DO_CAS         = 8
};

MemcachedStatus
MemcachedExec::unpack( void *buf,  size_t &buflen ) noexcept
{
  this->msg = (MemcachedMsg *) this->strm.alloc_temp( sizeof( MemcachedMsg ) );
  if ( this->msg == NULL )
    return MEMCACHED_ALLOC_FAIL;
  this->msg->cmd    = 0;
  this->msg->opcode = 0; /* only set when binary */
  this->msg->res    = 0;
  this->msg->pad    = 0xaa;
  this->msg->opaque = 0;
  this->msg->ini    = 0;
  this->msg->keycnt = 0;
  return this->msg->unpack( buf, buflen, this->strm.tmp );
}

const char *
rai::ds::memcached_cmd_string( uint8_t cmd ) noexcept
{
  switch ( (MemcachedCmd) ( cmd & MC_CMD_MASK ) ) {
    case MC_NONE: return "NONE";
    case MC_SET: return "SET";
    case MC_ADD: return "ADD";
    case MC_REPLACE: return "REPLACE";
    case MC_APPEND: return "APPEND";
    case MC_PREPEND: return "PREPEND";
    case MC_CAS: return "CAS";
    case MC_GET: return "GET";
    case MC_GETS: return "GETS";
    case MC_DELETE: return "DELETE";
    case MC_INCR: return "INCR";
    case MC_DECR: return "DECR";
    case MC_TOUCH: return "TOUCH";
    case MC_GAT: return "GAT";
    case MC_GATS: return "GATS";
    case MC_SLABS: return "SLABS";
    case MC_LRU: return "LRU";
    case MC_LRU_CRAWLER: return "LRU_CRAWLER";
    case MC_WATCH: return "WATCH";
    case MC_STATS: return "STATS";
    case MC_FLUSH_ALL: return "FLUSH_ALL";
    case MC_CACHE_MEMLIMIT: return "CACHE_MEMLIMIT";
    case MC_VERSION: return "VERSION";
    case MC_QUIT: return "QUIT";
    case MC_NO_OP: return "NO_OP";
    case MC_VERBOSITY: return "VERBOSITY";
    case MC_CMD_MASK:
    case MC_QUIET:
    case MC_KEY:
    case MC_BINARY: break;
  }
  return "unknown";
}

void
MemcachedMsg::print( void ) noexcept
{
  uint32_t i, j;
  printf( "%s", memcached_cmd_string( this->cmd ) );
  switch ( this->command() ) {
    case MC_SET:
    case MC_ADD:
    case MC_APPEND:
    case MC_REPLACE:
    case MC_PREPEND: /* SET key flags ttl msglen */
      printf( " %.*s %u %" PRIu64 " %" PRIu64 "",
              (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->flags, this->ttl, this->msglen );
      break;
    case MC_CAS:    /* CAS key flags ttl msglen cas-id */
      printf( " %.*s %u %" PRIu64 " %" PRIu64 " %" PRIu64 "",
              (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->flags, this->ttl, this->msglen, this->cas );
      break;
    case MC_GET:
    case MC_GETS: /* GET key [key2 ...] */
      i = this->first;
      for ( j = 0; j < this->keycnt; j++, i++ )
        printf( " %.*s", (int) this->args[ i ].len, this->args[ i ].str );
      break;
    case MC_GAT:
    case MC_GATS: /* GAT ttl key [key2 ...] */
      printf( " %" PRIu64 "", this->ttl );
      i = this->first;
      for ( j = 0; j < this->keycnt; j++, i++ )
        printf( " %.*s", (int) this->args[ i ].len, this->args[ i ].str );
      break;
    case MC_DELETE: /* DELETE key */
      printf( " %.*s", (int) this->args[ 0 ].len, this->args[ 0 ].str );
      break;
    case MC_TOUCH:  /* TOUCH key ttl */
      printf( " %.*s %" PRIu64 "", (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->ttl );
      break;
    case MC_DECR:
    case MC_INCR:   /* INC key val */
      printf( " %.*s %" PRIu64 "", (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->inc );
      break;
    default:
      break;
  }
  if ( ! this->is_quiet() == 0 )
    printf( " noreply" );
  printf( "\n" );
}

const char *
rai::ds::memcached_res_string( uint8_t res ) noexcept
{
  switch ( (MemcachedResult) res ) {
    case MR_NONE: return "NONE";
    case MR_END: return "END";
    case MR_DELETED: return "DELETED";
    case MR_STORED: return "STORED";
    case MR_VALUE: return "VALUE";
    case MR_INT: return "INT";
    case MR_TOUCHED: return "TOUCHED";
    case MR_NOT_FOUND: return "NOT_FOUND";
    case MR_NOT_STORED: return "NOT_STORED";
    case MR_EXISTS: return "EXISTS";
    case MR_ERROR: return "ERROR";
    case MR_CLIENT_ERROR: return "CLIENT_ERROR";
    case MR_SERVER_ERROR: return "SERVER_ERROR";
    case MR_BUSY: return "BUSY";
    case MR_BADCLASS: return "BADCLASS";
    case MR_NOSPARE: return "NOSPARE";
    case MR_NOTFULL: return "NOTFULL";
    case MR_UNSAFE: return "UNSAFE";
    case MR_SAME: return "SAME";
    case MR_OK: return "OK";
    case MR_STAT: return "STAT";
    case MR_VERSION: return "VERSION";
  }
  return "unknown";
}

void
MemcachedRes::print( void ) noexcept
{
  if ( this->res == MR_INT ) {
    printf( "%" PRIu64 "", this->ival );
  }
  else {
    printf( "%s", memcached_res_string( this->res ) );
    if ( this->res == MR_VALUE ) { /* VALUE key flags msglen [cas] */
      printf( " %.*s %u %" PRIu64 "",
              (int) this->keylen, this->key, this->flags, this->msglen );
      if ( this->argcnt == 4 )
        printf( " %" PRIu64 "", this->cas );
    }
  }
  printf( "\n" );
}

static void init_op_cmd( uint8_t *oc )
{
  oc[ OP_GET ]        = MC_GET;
  oc[ OP_SET ]        = MC_SET;
  oc[ OP_ADD ]        = MC_ADD;
  oc[ OP_REPLACE ]    = MC_REPLACE;
  oc[ OP_DELETE ]     = MC_DELETE;
  oc[ OP_INCREMENT ]  = MC_INCR;
  oc[ OP_DECREMENT ]  = MC_DECR;
  oc[ OP_QUIT ]       = MC_QUIT;
  oc[ OP_FLUSH ]      = MC_FLUSH_ALL;
  oc[ OP_GETQ ]       = MC_GET | MC_QUIET;

  oc[ OP_NO_OP ]      = MC_NO_OP;
  oc[ OP_VERSION ]    = MC_VERSION;
  oc[ OP_GETK ]       = MC_GET | MC_KEY;
  oc[ OP_GETKQ ]      = MC_GET | MC_KEY | MC_QUIET;
  oc[ OP_APPEND ]     = MC_APPEND;
  oc[ OP_PREPEND ]    = MC_PREPEND;
  oc[ OP_STAT ]       = MC_STATS;
  oc[ OP_SETQ ]       = MC_SET | MC_QUIET;
  oc[ OP_ADDQ ]       = MC_ADD | MC_QUIET;
  oc[ OP_REPLACEQ ]   = MC_REPLACE | MC_QUIET;

  oc[ OP_DELETEQ ]    = MC_DELETE | MC_QUIET;
  oc[ OP_INCREMENTQ ] = MC_INCR | MC_QUIET;
  oc[ OP_DECREMENTQ ] = MC_DECR | MC_QUIET;
  oc[ OP_QUITQ ]      = MC_QUIT | MC_QUIET;
  oc[ OP_FLUSHQ ]     = MC_FLUSH_ALL | MC_QUIET;
  oc[ OP_APPENDQ ]    = MC_APPEND | MC_QUIET;
  oc[ OP_PREPENDQ ]   = MC_PREPEND | MC_QUIET;
  oc[ OP_VERBOSITY ]  = MC_VERBOSITY;
  oc[ OP_TOUCH ]      = MC_TOUCH;
  oc[ OP_GAT ]        = MC_GAT;

  oc[ OP_GATQ ]       = MC_GAT | MC_QUIET;
  oc[ OP_SASL_LIST ]  = MC_NONE;
  oc[ OP_SASL_AUTH ]  = MC_NONE;
  oc[ OP_SASL_STEP ]  = MC_NONE;

  oc[ OP_RGET ]       = MC_NONE; /* range ops */
  oc[ OP_RSET ]       = MC_NONE;
  oc[ OP_RSETQ ]      = MC_NONE;
  oc[ OP_RAPPEND ]    = MC_NONE;
  oc[ OP_RAPPENDQ ]   = MC_NONE;
  oc[ OP_RPREPEND ]   = MC_NONE;

  oc[ OP_RPREPENDQ ]  = MC_NONE;
  oc[ OP_RDELETE ]    = MC_NONE;
  oc[ OP_RDELETEQ ]   = MC_NONE;
  oc[ OP_RINCR ]      = MC_NONE;
  oc[ OP_RINCRQ ]     = MC_NONE;
  oc[ OP_RDECR ]      = MC_NONE;
  oc[ OP_RDECRQ ]     = MC_NONE;
}

static inline MemcachedResult
str_to_result( const char *s,  size_t sz )
{
  if ( sz > 0 && s[ 0 ] >= '0' && s[ 1 ] <= '9' )
    return MR_INT;
  if ( sz < 4 ) {
    if ( sz == 2 && C4_KW( s[ 0 ], s[ 1 ], 0, 0 ) == MR_KW_OK )
      return MR_OK;
    if ( sz == 3 && C4_KW( s[ 0 ], s[ 1 ], s[ 2 ], 0 ) == MR_KW_END )
      return MR_END;
    return MR_NONE;
  }
  switch ( C4_KW( s[ 0 ], s[ 1 ], s[ 2 ], s[ 3 ] ) ) {
    case MR_KW_NOT_FOUND:
      if ( s[ 4 ] == 'F' || s[ 4 ] == 'f' )
        return MR_NOT_FOUND;
      return MR_NOT_STORED;
    case MR_KW_DELETED:      return MR_DELETED;
    case MR_KW_STORED:       return MR_STORED;
    case MR_KW_VALUE:        return MR_VALUE;
    case MR_KW_TOUCHED:      return MR_TOUCHED;
    case MR_KW_EXISTS:       return MR_EXISTS;
    case MR_KW_ERROR:        return MR_ERROR;
    case MR_KW_CLIENT_ERROR: return MR_CLIENT_ERROR;
    case MR_KW_SERVER_ERROR: return MR_SERVER_ERROR;
    case MR_KW_BUSY:         return MR_BUSY;
    case MR_KW_BADCLASS:     return MR_BADCLASS;
    case MR_KW_NOSPARE:      return MR_NOSPARE;
    case MR_KW_NOTFULL:      return MR_NOTFULL;
    case MR_KW_UNSAFE:       return MR_UNSAFE;
    case MR_KW_SAME:         return MR_SAME;
    case MR_KW_STAT:         return MR_STAT;
    case MR_KW_VERSION:      return MR_VERSION;
    default:                 return MR_NONE;
  }
}

static inline size_t
parse_mcargs( char *ptr,  char *end,  ScratchMem &wrk,  MemcachedArgs *&args )
{
  size_t incr = 1,
         cnt  = 0;

  while ( ptr < end && *ptr == ' ' )
    ptr++;
  if ( ptr == end )
    return 0;
  args[ 0 ].str = ptr;
  for (;;) {
    if ( ++ptr == end || *ptr == ' ' ) {
      args[ cnt ].len = ptr - args[ cnt ].str;
      cnt++;
      while ( ptr < end && *ptr == ' ' )
        ptr++; 
      if ( ptr == end )
        return cnt;
      if ( cnt == incr ) {
        size_t len = ( end - ptr ) / 8;
        incr += ( ( incr == 1 ) ? ( ( len | 3 ) + 1 ) : 16 );
        MemcachedArgs * args2 = (MemcachedArgs *)
          wrk.alloc( sizeof( MemcachedArgs ) * incr );
        if ( args2 == NULL )
          return 0;
        ::memcpy( args2, args, sizeof( MemcachedArgs ) * cnt );
        args = args2;
      }
      args[ cnt ].str = ptr;
    }
  }
}

MemcachedStatus
MemcachedMsg::unpack( void *buf,  size_t &buflen,  ScratchMem &wrk ) noexcept
{
  char  * ptr = (char *) buf,
        * eol,
        * end,
        * s;
  size_t  i, j = 0, k;
  MemcachedStatus r;

  this->cmd = MC_NONE;

  for (;;) {
    size_t n = buflen - j;
    if ( n == 0 )
      return MEMCACHED_MSG_PARTIAL;
    if ( (uint8_t) ptr[ j ] == 0x80 ) {
      static uint8_t bin_to_ascii_cmd[ 256 ];
      MemcachedBinHdr b;
      if ( n < sizeof( b ) )
        return MEMCACHED_MSG_PARTIAL;
      ::memcpy( &b, &ptr[ j ], sizeof( b ) );
      ptr = &ptr[ j + sizeof( b ) ];
      n -= sizeof( b );
      this->opcode = b.opcode;
      this->opaque = b.opaque;
      this->cmd = bin_to_ascii_cmd[ b.opcode ] | MC_BINARY;
      if ( this->cmd == MC_BINARY ) {
        init_op_cmd( bin_to_ascii_cmd );
        this->cmd = bin_to_ascii_cmd[ b.opcode ] | MC_BINARY;
      }
      /*printf( "unpack: [%s]\n", memcached_cmd_string( this->cmd ) );*/
      switch ( this->command() ) {
        case MC_SET:
        case MC_ADD:
        case MC_REPLACE:   r = this->parse_bin_store( b, ptr, n ); break;
        case MC_APPEND:
        case MC_PREPEND:   r = this->parse_bin_pend( b, ptr, n ); break;
        case MC_DELETE:
        case MC_GET:       r = this->parse_bin_retr( b, ptr, n ); break;
        case MC_GAT:
        case MC_TOUCH:     r = this->parse_bin_touch( b, ptr, n ); break;
        case MC_DECR:
        case MC_INCR:      r = this->parse_bin_incr( b, ptr, n ); break;
        case MC_FLUSH_ALL: r = this->parse_bin_op( b, ptr, n, 4 ); break;
        case MC_VERSION:   r = this->parse_bin_op( b, ptr, n, 0 ); break;
        case MC_QUIT:      r = this->parse_bin_op( b, ptr, n, 0 ); break;
        case MC_NO_OP:     r = this->parse_bin_op( b, ptr, n, 0 ); break;
        case MC_VERBOSITY: r = this->parse_bin_op( b, ptr, n, 4 ); break;
        default:
          r = this->parse_bin_op( b, ptr, n, 0 );
          if ( r == MEMCACHED_OK ) /* command unknown */
            r = MEMCACHED_BAD_BIN_CMD;
          break;
      }
      if ( r != MEMCACHED_MSG_PARTIAL ) {
        buflen = j + sizeof( b ) + n;
        if ( r >= MEMCACHED_BAD_BIN_ARGS ) {
          this->cmd = MC_BINARY;
          this->res = r;
        }
        return MEMCACHED_OK;
      }
      return MEMCACHED_MSG_PARTIAL;
    }
    eol = (char *) ::memchr( &ptr[ j ], '\n', n );
    if ( eol == NULL )
      return MEMCACHED_MSG_PARTIAL;

    i = eol - ptr;
    k = j;
    j = i + 1;
    if ( i > 0 && ptr[ i - 1 ] == '\r' )
      i--;

    for (;;) {
      if ( k == i )
        break;
      if ( ptr[ k ] == ' ' )
        k++;
      else {
        end = &ptr[ i ];
        ptr = &ptr[ k ];
        goto not_an_empty_line;
      }
    }
  }
not_an_empty_line:;
  for ( s = ptr; ; ptr++ ) {
    if ( ptr >= end ) {
      buflen = j;
      if ( &s[ 4 ] > ptr )
        return MEMCACHED_EMPTY;
      break;
    }
    if ( *ptr == ' ' )
      break;
  }
  uint32_t kw = C4_KW( s[ 0 ], s[ 1 ], s[ 2 ], s[ 3 ] );
  
  this->args   = &this->xarg;
  this->argcnt = (uint32_t) parse_mcargs( ptr, end, wrk, this->args );
  size_t bytes_left = buflen - j;
  buflen = j;

  switch ( kw ) {
    case MC_KW_SET:     this->cmd = MC_SET;       break;
    case MC_KW_ADD:     this->cmd = MC_ADD;       break;
    case MC_KW_APPEND:  this->cmd = MC_APPEND;    break;
    case MC_KW_REPLACE: this->cmd = MC_REPLACE;   break;
    case MC_KW_PREPEND: this->cmd = MC_PREPEND;   break;
    case MC_KW_CAS:     this->cmd = MC_CAS;       break;
    case MC_KW_GET:     this->cmd = MC_GET;       return this->parse_retr();
    case MC_KW_GETS:    this->cmd = MC_GETS;      return this->parse_retr();
    case MC_KW_GAT:     this->cmd = MC_GAT;       return this->parse_gat();
    case MC_KW_GATS:    this->cmd = MC_GATS;      return this->parse_gat();
    case MC_KW_DELETE:  this->cmd = MC_DELETE;    return this->parse_del();
    case MC_KW_TOUCH:   this->cmd = MC_TOUCH;     return this->parse_touch();
    case MC_KW_DECR:    this->cmd = MC_DECR;      return this->parse_incr();
    case MC_KW_INCR:    this->cmd = MC_INCR;      return this->parse_incr();
    case MC_KW_SLABS:   this->cmd = MC_SLABS;     return MEMCACHED_OK;
    case MC_KW_LRU:     this->cmd = MC_LRU;       return MEMCACHED_OK;
    case MC_KW_LRU_CRAWLER: this->cmd = MC_LRU_CRAWLER; return MEMCACHED_OK;
    case MC_KW_WATCH:   this->cmd = MC_WATCH;     return MEMCACHED_OK;
    case MC_KW_STATS:   this->cmd = MC_STATS;     return MEMCACHED_OK;
    case MC_KW_FLUSH_ALL: this->cmd = MC_FLUSH_ALL; return MEMCACHED_OK;
    case MC_KW_CACHE_MEMLIMIT: this->cmd = MC_CACHE_MEMLIMIT; return MEMCACHED_OK;
    case MC_KW_VERSION: this->cmd = MC_VERSION;   return MEMCACHED_OK;
    case MC_KW_QUIT:    this->cmd = MC_QUIT;      return MEMCACHED_OK;
    case MC_KW_NO_OP:   this->cmd = MC_NO_OP;     return MEMCACHED_OK;
    default:            this->cmd = MC_NONE;      return MEMCACHED_BAD_CMD;
  }
  if ( kw != MC_KW_CAS )
    r = this->parse_store();
  else
    r = this->parse_cas();
  if ( r != MEMCACHED_OK )
    return r;
  size_t n = this->msglen;
  if ( bytes_left < n )
    return MEMCACHED_MSG_PARTIAL;

  this->msg = &eol[ 1 ];
  bytes_left -= n;
  /* eat '\r\n' */
  if ( bytes_left > 0 && this->msg[ this->msglen ] < ' ' ) {
    n++;
    if ( bytes_left > 1 && this->msg[ this->msglen + 1 ] < ' ' )
      n++;
  }
  buflen += n;
  return MEMCACHED_OK;
}

static inline MemcachedStatus
get_u64( const char *str,  size_t len,  uint64_t &val )
{
  switch ( string_to_uint( str, len, val ) ) {
    default:                   return MEMCACHED_OK;
    case STR_CVT_INT_OVERFLOW: return MEMCACHED_INT_OVERFLOW;
    case STR_CVT_BAD_INT:      return MEMCACHED_BAD_INT;
  }
}

static inline MemcachedStatus
get_u32( const char *str,  size_t len,  uint32_t &val )
{
  uint64_t u64;
  MemcachedStatus r = get_u64( str, len, u64 );
  if ( r == MEMCACHED_OK ) {
    if ( u64 > 0xffffffffU )
      return MEMCACHED_INT_OVERFLOW;
    val = (uint32_t) u64;
  }
  return r;
}

MemcachedStatus
MemcachedRes::unpack( void *buf,  size_t &buflen,  ScratchMem &wrk ) noexcept
{
  char  * s = (char *) buf,
        * eol;
  size_t  i, j, n;
  MemcachedStatus r = MEMCACHED_OK;

  n = buflen;
  if ( n == 0 )
    return MEMCACHED_MSG_PARTIAL;
  eol = (char *) ::memchr( s, '\n', n );
  if ( eol == NULL )
    return MEMCACHED_MSG_PARTIAL;

  i = eol - s;
  j = i + 1;
  if ( i > 0 && s[ i - 1 ] == '\r' )
    i--;
  MemcachedResult result = str_to_result( s, i );
  if ( result != MR_NONE ) {
    #define b( x ) ( (uint64_t) 1 << (int) x )
    static uint64_t has_args = b( MR_VALUE ) | b( MR_STAT ) | b( MR_VERSION ),
                    is_error = b( MR_ERROR ) | b( MR_CLIENT_ERROR ) |
                               b( MR_SERVER_ERROR );
    this->zero( (uint8_t) result );

    if ( ( has_args & b( result ) ) != 0 ) {
      char * ptr = &s[ 4 ],
           * end = &s[ i ];
      while ( ptr < end )
        if ( *ptr++ == ' ' )
          break;
      this->args   = &this->xarg;
      this->argcnt = (uint32_t) parse_mcargs( ptr, end, wrk, this->args );
      if ( result == MR_VALUE ) {
        r = this->parse_value_result();
        if ( r == MEMCACHED_OK ) {
          if ( j + this->msglen + 2 > n )
            return MEMCACHED_MSG_PARTIAL;
          this->msg = &eol[ 1 ];
          j += this->msglen + 2;
        }
      }
    }
    else if ( result == MR_INT ) {
      r = get_u64( s, i, this->ival );
    }
    else {
      this->is_err = ( ( is_error & b( result ) ) != 0 );
    }
    #undef b
  }
  else {
    r = MEMCACHED_BAD_RESULT;
  }
  buflen = j;
  return r;
}

MemcachedStatus
MemcachedRes::parse_value_result( void ) noexcept
{
  MemcachedStatus r;
  /* VALUE key flags msglen [cas] */
  if ( this->argcnt != 3 && this->argcnt != 4 )
    return MEMCACHED_BAD_ARGS;
  this->key    = this->args[ 0 ].str;
  this->keylen = (uint16_t) this->args[ 0 ].len;
  r = get_u32( this->args[ 1 ].str, this->args[ 1 ].len, this->flags );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 2 ].str, this->args[ 2 ].len, this->msglen );
  if ( this->argcnt == 4 )
    r = get_u64( this->args[ 3 ].str, this->args[ 3 ].len, this->cas );
  if ( r != MEMCACHED_OK )
    return r;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_store( void ) noexcept
{
  MemcachedStatus r;
  /* SET key flags ttl msglen [noreply] */
  if ( this->argcnt != 4 && this->argcnt != 5 )
    return MEMCACHED_BAD_ARGS;
  r = get_u32( this->args[ 1 ].str, this->args[ 1 ].len, this->flags );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 2 ].str, this->args[ 2 ].len, this->ttl );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 3 ].str, this->args[ 3 ].len, this->msglen );
  if ( r != MEMCACHED_OK )
    return r;
  this->keycnt  = 1;
  this->first   = 0;
  this->cmd    |= ( ( this->argcnt == 5 ) ? MC_QUIET : 0 );
  this->cas     = 0;
  this->inc     = 0;
  this->msg     = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_cas( void ) noexcept
{
  MemcachedStatus r;
  /* CAS key flags ttl msglen cas-unique-id [noreply] */
  if ( this->argcnt != 5 && this->argcnt != 6 )
    return MEMCACHED_BAD_ARGS;
  r = get_u32( this->args[ 1 ].str, this->args[ 1 ].len, this->flags );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 2 ].str, this->args[ 2 ].len, this->ttl );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 3 ].str, this->args[ 3 ].len, this->msglen );
  if ( r == MEMCACHED_OK )
    r = get_u64( this->args[ 4 ].str, this->args[ 4 ].len, this->cas );
  if ( r != MEMCACHED_OK )
    return r;
  this->keycnt  = 1;
  this->first   = 0;
  this->inc     = 0;
  this->msg     = NULL;
  this->cmd    |= ( ( this->argcnt == 6 ) ? MC_QUIET : 0 );
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_retr( void ) noexcept
{
  /* GET key [key2] ... */
  if ( this->argcnt == 0 )
    return MEMCACHED_BAD_ARGS;
  this->keycnt  = this->argcnt;
  this->first   = 0;
  this->ttl     = 0;
  this->cas     = 0;
  this->msglen  = 0;
  this->inc     = 0;
  this->msg     = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_gat( void ) noexcept
{
  MemcachedStatus r;
  /* GAT ttl key [key2] ... */
  if ( this->argcnt <= 1 )
    return MEMCACHED_BAD_ARGS;
  r = get_u64( this->args[ 0 ].str, this->args[ 0 ].len, this->ttl );
  if ( r != MEMCACHED_OK )
    return r;
  this->keycnt  = this->argcnt - 1;
  this->first   = 1;
  this->cas     = 0;
  this->msglen  = 0;
  this->inc     = 0;
  this->msg     = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_del( void ) noexcept
{
  /* DELETE key [noreply] */
  if ( this->argcnt != 1 && this->argcnt != 2 )
    return MEMCACHED_BAD_ARGS;
  this->keycnt  = 1;
  this->first   = 0;
  this->ttl     = 0;
  this->cas     = 0;
  this->msglen  = 0;
  this->inc     = 0;
  this->msg     = NULL;
  this->cmd    |= ( ( this->argcnt == 2 ) ? MC_QUIET : 0 );
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_incr( void ) noexcept
{
  MemcachedStatus r;
  /* INCR key value [noreply] */
  if ( this->argcnt != 2 && this->argcnt != 3 )
    return MEMCACHED_BAD_ARGS;
  r = get_u64( this->args[ 1 ].str, this->args[ 1 ].len, this->inc );
  if ( r != MEMCACHED_OK )
    return r;
  this->keycnt  = 1;
  this->first   = 0;
  this->cas     = 0;
  this->ttl     = 0;
  this->msglen  = 0;
  this->msg     = NULL;
  this->cmd    |= ( ( this->argcnt == 3 ) ? MC_QUIET : 0 );
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_touch( void ) noexcept
{
  MemcachedStatus r;
  /* TOUCH key ttl [noreply] */
  if ( this->argcnt != 2 && this->argcnt != 3 )
    return MEMCACHED_BAD_ARGS;
  r = get_u64( this->args[ 1 ].str, this->args[ 1 ].len, this->ttl );
  if ( r != MEMCACHED_OK )
    return r;
  this->keycnt  = 1;
  this->first   = 0;
  this->cas     = 0;
  this->msglen  = 0;
  this->inc     = 0;
  this->msg     = NULL;
  this->cmd    |= ( ( this->argcnt == 3 ) ? MC_QUIET : 0 );
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_store( MemcachedBinHdr &b,  char *ptr,
                               size_t &buflen ) noexcept
{
  /* SET, ADD, REPLACE:  has key, has extra flags+ttl, may have msg */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;
  uint32_t ex[ 2 ];

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = datalen;
  /* data must be >= key + extras, have a key, extras must be flags + ttl */
  if ( datalen < keylen + exlen || keylen == 0 || exlen != sizeof( ex ) )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = &this->xarg;
  this->argcnt   = 1;
  this->keycnt   = 1;
  this->first    = 0;
  ::memcpy( ex, ptr, sizeof( ex ) );
  ptr = &ptr[ sizeof( ex ) ];
  this->flags    = kv_bswap32( ex[ 0 ] );
  this->ttl      = kv_bswap32( ex[ 1 ] );
  this->cas      = kv_bswap64( b.cas );
  this->msglen   = datalen - ( exlen + keylen );
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = &ptr[ keylen ]; /* the msg data */
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_pend( MemcachedBinHdr &b,  char *ptr,
                              size_t &buflen ) noexcept
{
  /* APPEND, PREPEND:  has key, has msg */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = datalen;
  /* data must be >= keylen, have a key, extras must zero */
  if ( datalen < keylen || keylen == 0 || exlen != 0 )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = &this->xarg;
  this->argcnt   = 1;
  this->keycnt   = 1;
  this->first    = 0;
  this->flags    = 0;
  this->ttl      = 0;
  this->cas      = 0;
  this->msglen   = datalen - keylen;
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = &ptr[ keylen ]; /* the msg data */
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_retr( MemcachedBinHdr &b,  char *ptr,
                              size_t &buflen ) noexcept
{
  /* DELETE, GET, GETQ, GETK, GETKQ:  has key */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = keylen;
  /* data must be == keylen, have a key, extras must zero */
  if ( datalen != keylen || keylen == 0 || exlen != 0 )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = &this->xarg;
  this->argcnt   = 1;
  this->keycnt   = 1;
  this->first    = 0;
  this->flags    = 0;
  this->ttl      = 0;
  this->cas      = 0;
  this->msglen   = 0;
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_touch( MemcachedBinHdr &b,  char *ptr,
                               size_t &buflen ) noexcept
{
  /* TOUCH, GAT, GATQ:  has key, has extra ttl */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;
  uint32_t ex[ 1 ];

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = datalen;
  /* data must be == keylen + extras, have a key, extras must be ttl */
  if ( datalen != keylen + exlen || keylen == 0 || exlen != 4 )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = &this->xarg;
  this->argcnt   = 1;
  this->keycnt   = 1;
  this->first    = 0;
  this->flags    = 0;
  ::memcpy( ex, ptr, sizeof( ex ) );
  ptr = &ptr[ sizeof( ex ) ];
  this->ttl      = kv_bswap32( ex[ 0 ] );
  this->cas      = 0;
  this->msglen   = 0;
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_incr( MemcachedBinHdr &b,  char *ptr,
                              size_t &buflen ) noexcept
{
  /* INCR, DECR:  has key, has extra inc+ini+ttl */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;
  uint32_t ex[ 1 ];

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = datalen;
  /* data must be == keylen + extras, have a key, extras must be inc+ini+ttl */
  if ( datalen != keylen + exlen || keylen == 0 || exlen != 20 )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = &this->xarg;
  this->argcnt   = 1;
  this->keycnt   = 1;
  this->first    = 0;
  this->flags    = 0;
  ::memcpy( &this->inc, ptr, sizeof( this->inc ) );
  this->inc = kv_bswap64( this->inc );
  ::memcpy( &this->ini, &ptr[ 8 ], sizeof( this->ini ) );
  this->ini = kv_bswap64( this->ini );/* initial value */
  ::memcpy( ex, &ptr[ 16 ], sizeof( ex ) );
  this->ttl = kv_bswap32( ex[ 0 ] );
  ptr = &ptr[ 20 ];
  this->msglen   = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_op( MemcachedBinHdr &b,  char *ptr,  size_t &buflen,
                            size_t extra_sz ) noexcept
{
  /* QUIT, VERSION:  no key, may have extra */
  size_t keylen  = kv_bswap16( b.keylen ),
         datalen = kv_bswap32( b.datalen ),
         exlen   = b.extralen;
  uint32_t ex[ 1 ];

  if ( datalen > buflen )
    return MEMCACHED_MSG_PARTIAL;
  buflen = datalen;
  /* data must be == keylen + extras, have a key, extras must be inc+ini+ttl */
  if ( datalen != extra_sz || keylen != 0 || exlen != extra_sz )
    return MEMCACHED_BAD_BIN_ARGS;
  this->args     = NULL;
  this->argcnt   = 0;
  this->keycnt   = 0;
  this->first    = 0;
  this->flags    = 0;
  this->inc      = 0;
  if ( extra_sz == 4 ) {
    ::memcpy( ex, ptr, 4 );
    this->ttl = kv_bswap32( ex[ 0 ] );
  }
  else {
    this->ttl = 0;
  }
  this->msglen   = 0;
  this->msg      = NULL;
  return MEMCACHED_OK;
}

void
MemcachedExec::send_err( int status,  KeyStatus kstatus ) noexcept
{
  size_t sz = 0;
  switch ( (MemcachedStatus) status ) {
    case MEMCACHED_OK:          break;
    case MEMCACHED_MSG_PARTIAL: break;
    case MEMCACHED_EMPTY:       break;
    case MEMCACHED_SETUP_OK:    break;
    case MEMCACHED_SUCCESS:     break;
    case MEMCACHED_CONTINUE:    break;
    case MEMCACHED_DEPENDS:     break;
    case MEMCACHED_QUIT:        break;
    case MEMCACHED_VERSION:     break;
    case MEMCACHED_BAD_RESULT:  break;
    case MEMCACHED_ALLOC_FAIL: {
      static const char fail[] = "SERVER_ERROR alloc failed";
      if ( this->msg != NULL && this->msg->is_binary() )
        sz = this->send_bin_status( MS_ALLOC_FAIL );
      else
        sz = this->send_string( fail, sizeof( fail ) - 1 );
      break;
    }
    case MEMCACHED_BAD_CMD: {
      static const char bad[] = "CLIENT_ERROR bad command";
      sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_ARGS: {
      static const char bad[] = "CLIENT_ERROR bad args";
      sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_INT: {
      static const char bad[] = "CLIENT_ERROR bad integer";
      sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_INT_OVERFLOW: {
      static const char bad[] = "CLIENT_ERROR integer overflow";
      sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_TYPE: {
      static const char fail[] = "CLIENT_ERROR bad type";
      sz = this->send_string( fail, sizeof( fail ) - 1 );
      break;
    }
    case MEMCACHED_NOT_IMPL: {
      static const char bad[] = "SERVER_ERROR not implemented";
      if ( this->msg != NULL && this->msg->is_binary() )
        sz = this->send_bin_status( MS_NOT_SUPPORTED );
      else
        sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_INCR: {
      static const char bad[] = "CLIENT_ERROR cannot increment or decrement "
                                "non-numeric value";
      sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_PAD: {
      static const char bad[] = "SERVER_ERROR bad pad";
      if ( this->msg != NULL && this->msg->is_binary() )
        sz = this->send_bin_status( MS_INTERNAL_ERROR );
      else
        sz = this->send_string( bad, sizeof( bad ) - 1 );
      break;
    }
    case MEMCACHED_BAD_BIN_ARGS:
      sz = this->send_bin_status( MS_BAD_ARGS );
      break;
    case MEMCACHED_BAD_BIN_CMD:
      sz = this->send_bin_status( MS_UNKNOWN_COMMAND );
      break;
    case MEMCACHED_ERR_KV:
      if ( this->msg != NULL && this->msg->is_binary() )
        sz = this->send_bin_status( MS_INTERNAL_ERROR );
      else
        sz = this->send_err_kv( kstatus );
      break;
  }
  this->strm.sz += sz;
}

size_t
MemcachedExec::send_string( const void *s ) noexcept
{
  return this->send_string( s, ::strlen( (const char *) s ) );
}

size_t 
MemcachedExec::send_string( const void *s,  size_t slen ) noexcept
{
  char * buf = this->strm.alloc( slen + 2 );

  if ( buf != NULL ) {
    ::memcpy( buf, s, slen );
    buf[ slen ] = '\r';
    buf[ slen + 1 ] = '\n';
    return slen + 2;
  }
  return 0;
}

size_t 
MemcachedExec::send_bin_status( uint16_t status,  const void *s,
                                size_t slen ) noexcept
{
  if ( s == NULL ) {
    /* strings copied from memcached.c:1430 */
    switch ( (MemcachedBinStatus) status ) {
      case MS_OK:              s = "Ok"; break;
      case MS_NOT_FOUND:       s = "Not found"; break;
      case MS_VALUE_TOO_BIG:   s = "Too large."; break;
      case MS_BAD_ARGS:        s = "Invalid arguments"; break;
      case MS_EXISTS:          s = "Data exists for key."; break;
      case MS_NOT_STORED:      s = "Not stored."; break;
      case MS_BAD_INCR_VALUE:  s = "Non-numeric server-side value for incr or decr"; break;
      case MS_BAD_VBUCKET:     s = "Bad vbucket"; break;
      case MS_AUTH_ERROR:      s = "Auth failure."; break;
      case MS_AUTH_CONTINUE:   s = "Continue"; break;
      case MS_UNKNOWN_COMMAND: s = "Unknown command"; break;
      case MS_ALLOC_FAIL:      s = "Out of memory"; break;
      case MS_NOT_SUPPORTED:   s = "Not supported"; break;
      case MS_INTERNAL_ERROR:  s = "Internal error"; break;
      case MS_BUSY:            s = "Busy"; break;
      case MS_TEMP_FAIL:       s = "Temporary failure"; break;
    }
    if ( s == NULL )
      s = "Unknown";
  }
  if ( slen == 0 )
    slen = ::strlen( (const char *) s );
  char * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) + slen );

  if ( buf != NULL ) {
    MemcachedBinHdr hdr;
    ::memset( &hdr, 0, sizeof( hdr ) );
    hdr.magic   = 0x81;
    hdr.opcode  = this->msg->opcode;
    hdr.opaque  = this->msg->opaque; /* could be a serial number */
    hdr.status  = kv_bswap16( status );
    hdr.datalen = kv_bswap32( (uint32_t) slen );
    ::memcpy( buf, &hdr, sizeof( hdr ) );
    ::memcpy( &buf[ sizeof( hdr ) ], s, slen );
    return sizeof( hdr ) + slen;
  }
  return 0;
}

size_t 
MemcachedExec::send_bin_status_key( uint16_t status,  EvKeyCtx &ctx ) noexcept
{
  char   * key    = ctx.kbuf.u.buf;
  uint16_t keylen = ctx.kbuf.keylen - 1;
  char   * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) + keylen );

  if ( buf != NULL ) {
    MemcachedBinHdr hdr;
    ::memset( &hdr, 0, sizeof( hdr ) );
    hdr.magic   = 0x81;
    hdr.opcode  = this->msg->opcode;
    hdr.opaque  = this->msg->opaque; /* could be a serial number */
    hdr.status  = kv_bswap16( status );
    hdr.keylen  = kv_bswap16( keylen );
    hdr.datalen = kv_bswap32( keylen );
    ::memcpy( buf, &hdr, sizeof( hdr ) );
    ::memcpy( &buf[ sizeof( hdr ) ], key, keylen );
    return sizeof( hdr ) + keylen;
  }
  return 0;
}

size_t
MemcachedExec::send_err_kv( KeyStatus kstatus ) noexcept
{
  size_t bsz = 256;
  char * buf = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    bsz = ::snprintf( buf, bsz, "SERVER_ERROR KeyCtx %d/%s %s\r\n",
                     kstatus, kv_key_status_string( (KeyStatus) kstatus ),
                     kv_key_status_description( (KeyStatus) kstatus ) );
    return min_int( bsz, (size_t) 255 );
  }
  return 0;
}

MemcachedStatus
MemcachedExec::exec_key_setup( EvSocket *own,  EvPrefetchQueue *q,
                               EvKeyCtx *&ctx,  uint32_t n,
                               uint32_t idx ) noexcept
{
  const char * key    = this->msg->args[ n ].str;
  size_t       keylen = this->msg->args[ n ].len;
  void *p = this->strm.alloc_temp( EvKeyCtx::size( keylen ) );
  if ( p == NULL )
    return MEMCACHED_ALLOC_FAIL;
  ctx = new ( p ) EvKeyCtx( this->kctx.ht, own, key, keylen, n, idx, this->hs );
  if ( q != NULL && ! q->push( ctx ) )
    return MEMCACHED_ALLOC_FAIL;
  ctx->status = MEMCACHED_CONTINUE;
  return MEMCACHED_SETUP_OK;
}

MemcachedStatus
MemcachedExec::exec( EvSocket *svc,  EvPrefetchQueue *q ) noexcept
{
  if ( this->msg->pad != 0xaa ) {
    return MEMCACHED_BAD_PAD;
  }
  if ( this->msg->keycnt > 0 ) {
    uint32_t i   = this->msg->first,
             cnt = this->msg->keycnt;
    MemcachedStatus status;

    this->key_cnt  = 1;
    this->key_done = 0;

    this->key  = NULL;
    this->keys = &this->key;
    /* setup first key */
    status = this->exec_key_setup( svc, q, this->key, i, 0 );
    if ( status == MEMCACHED_SETUP_OK ) {
      /* setup rest of keys, if any */
      if ( cnt > 1 ) {
        this->keys = (EvKeyCtx **)
          this->strm.alloc_temp( sizeof( this->keys[ 0 ] ) * cnt );
        if ( this->keys == NULL )
          status = MEMCACHED_ALLOC_FAIL;
        else {
          this->keys[ 0 ] = this->key;
          do {
            status = this->exec_key_setup( svc, q, this->keys[ this->key_cnt ],
                                           ++i, this->key_cnt );
            this->key_cnt++;
          } while ( status == MEMCACHED_SETUP_OK && this->key_cnt  < cnt );
        }
      }
    }
    return status; /* cmds with keys return setup ok */
  }
  bool ( MemcachedExec::*fptr )( void ) = NULL;
  switch ( this->msg->command() ) {
    case MC_STATS:
      if ( this->msg->argcnt == 0 )
        this->put_stats();
      else if ( this->msg->match_arg( "settings", 8 ) )
        this->put_stats_settings();
      else if ( this->msg->match_arg( "items", 5 ) )
        this->put_stats_items();
      else if ( this->msg->match_arg( "sizes", 5 ) )
        this->put_stats_sizes();
      else if ( this->msg->match_arg( "slabs", 5 ) )
        this->put_stats_slabs();
      else if ( this->msg->match_arg( "conns", 5 ) )
        this->put_stats_conns();
      if ( this->strm.sz == 0 )
        this->strm.sz += this->send_string( mc_error, mc_error_len );
      else
        this->strm.sz += this->send_string( mc_end, mc_end_len );
      return MEMCACHED_OK;

    case MC_SLABS:          fptr = &MemcachedExec::do_slabs; break;
    case MC_LRU:            fptr = &MemcachedExec::do_lru; break;
    case MC_LRU_CRAWLER:    fptr = &MemcachedExec::do_lru_crawler; break;
    case MC_WATCH:          fptr = &MemcachedExec::do_watch; break;
    case MC_FLUSH_ALL:      fptr = &MemcachedExec::do_flush_all; break;
    case MC_CACHE_MEMLIMIT: fptr = &MemcachedExec::do_memlimit; break;
    case MC_NO_OP:
      this->do_no_op();
      break;

    case MC_VERSION: {
      const char *ver = kv_stringify( DS_VER );
      this->strm.sz += ( this->msg->is_binary() ?
                         this->send_bin_status( MS_OK, ver ) :
                         this->send_string( ver ) );
      return MEMCACHED_OK;
    }
    case MC_QUIT:
      if ( this->msg->is_binary() )
        this->strm.sz += this->send_bin_status( MS_OK, "", 0 );
      return MEMCACHED_QUIT;
    case MC_NONE:
      if ( this->msg->cmd == MC_BINARY ) /* no command */
        return (MemcachedStatus) this->msg->res; /* bin error code */
      /* FALLTHRU */
    default:
      return MEMCACHED_NOT_IMPL; /* otherwise ERROR */
  }
  if ( fptr != NULL ) {
    if ( ( this->*fptr )() ) {
      if ( this->msg->is_binary() )
        this->strm.sz += this->send_bin_status( MS_OK, "", 0 );
      else
        this->strm.sz += this->send_string( mc_ok, mc_ok_len );
    }
    else {
      this->strm.sz += this->send_string( mc_error, mc_error_len );
    }
  }
  return MEMCACHED_OK;
}

kv::KeyStatus
MemcachedExec::exec_key_fetch( EvKeyCtx &ctx,  bool force_read ) noexcept
{
  if ( test_read_only( this->msg->cmd ) || force_read ) {
    ctx.kstatus = this->kctx.find( &this->wrk );
    ctx.flags  |= EKF_IS_READ_ONLY;
  }
  else if ( test_mutator( this->msg->cmd ) != 0 ) {
#ifdef USE_EVICT_ACQUIRE
    if ( this->kv_load >= this->kv_crit )
      this->kctx.set( KEYCTX_EVICT_ACQUIRE );
    else
      this->kctx.clear( KEYCTX_EVICT_ACQUIRE );
#endif
    ctx.kstatus = this->kctx.acquire( &this->wrk );
    ctx.flags  |= ( ( ctx.kstatus == KEY_IS_NEW ) ? EKF_IS_NEW : 0 );
    ctx.flags  &= ~EKF_IS_READ_ONLY;
  }
  else {
    ctx.kstatus = KEY_NO_VALUE;
    ctx.status  = MEMCACHED_BAD_CMD;
    ctx.flags  |= EKF_IS_READ_ONLY;
  }
  if ( ctx.kstatus == KEY_OK ) /* not new and is found */
    ctx.type = this->kctx.get_type();
  return ctx.kstatus;
}

MemcachedStatus
MemcachedExec::exec_key_continue( EvKeyCtx &ctx ) noexcept
{
  if ( this->msg->pad != 0xaa ) {
    ctx.status = MEMCACHED_BAD_PAD;
    goto skip_key;
  }
  /* there is no MEMCACHED_DEPENDS case yet */
  if ( ctx.status != MEMCACHED_CONTINUE && ctx.status != MEMCACHED_DEPENDS ) {
    if ( ++this->key_done < this->key_cnt )
      return MEMCACHED_CONTINUE;
    return MEMCACHED_SUCCESS;
  }
  this->exec_key_set( ctx );
  /*if ( this->kctx.kbuf != &ctx.kbuf ||
       this->kctx.key != ctx.hash1 || this->kctx.key2 != ctx.hash2 )
    this->exec_key_prefetch( ctx );*/
  for (;;) {
    switch ( this->msg->command() ) {
      case MC_SET:
      case MC_ADD:
      case MC_APPEND:
      case MC_REPLACE:
      case MC_PREPEND: /* GET key flags ttl msglen */
      case MC_CAS:     /* CAS key flags ttl msglen cas-id */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_store( ctx ) :
                       this->exec_store( ctx ) );
        break;
      case MC_GET:
      case MC_GETS: /* GET key [key2 ...] */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_retr( ctx ) :
                       this->exec_retr( ctx ) );
        break;
      case MC_GAT:
      case MC_GATS: /* GAT ttl key [key2 ...] */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_retr_touch( ctx ) :
                       this->exec_retr_touch( ctx ) );
        break;
      case MC_DELETE: /* DELETE key */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_del( ctx ) :
                       this->exec_del( ctx ) );
        break;
      case MC_TOUCH:  /* TOUCH key ttl */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_touch( ctx ) :
                       this->exec_touch( ctx ) );
        break;
      case MC_DECR:
      case MC_INCR:   /* INC key val */
        ctx.status = ( this->msg->is_binary() ?
                       this->exec_bin_incr( ctx ) :
                       this->exec_incr( ctx ) );
        break;
      default:
        ctx.status = MEMCACHED_NOT_IMPL;
        break;
    }
    /* set the type when key is new */
    if ( ! ctx.is_read_only() ) {
      if ( ctx.is_new() && ctx.status <= MEMCACHED_SUCCESS ) {
        uint8_t type;
        if ( (type = ctx.type) == MD_NODATA )
          type = MD_STRING;
        this->kctx.set_type( type );
      }
      this->kctx.release();
    }
    /* if key depends on other keys */
    if ( ctx.status == MEMCACHED_DEPENDS ) {
      ctx.dep++;
      return MEMCACHED_DEPENDS;
    }
    /* continue if read key mutated while running */
    if ( ctx.status != MEMCACHED_ERR_KV || ctx.kstatus != KEY_MUTATED )
      break;
  }
skip_key:;
  if ( ++this->key_done < this->key_cnt ) {
    if ( memcached_status_success( ctx.status ) )
      return MEMCACHED_CONTINUE;
    for ( uint32_t i = 0; i < this->key_cnt; i++ )
      this->keys[ i ]->status = ctx.status;
  }
  else if ( this->key_cnt > 1 ) {
    if ( memcached_status_success( ctx.status ) )
      this->multi_get_send(); /* get is the only oper that has multiple keys */
    return MEMCACHED_SUCCESS;
  }
  switch ( ctx.status ) {
    case MEMCACHED_OK: break;
    default: this->send_err( ctx.status, ctx.kstatus ); break;
  }
  if ( this->key_done < this->key_cnt )
    return MEMCACHED_CONTINUE;
  return MEMCACHED_SUCCESS;
}

void
MemcachedExec::exec_run_to_completion( void ) noexcept
{
  if ( this->key_cnt == 1 ) { /* only one key */
    while ( this->key->status == MEMCACHED_CONTINUE ||
            this->key->status == MEMCACHED_DEPENDS )
      if ( this->exec_key_continue( *this->key ) == MEMCACHED_SUCCESS )
        break;
  }
  else {
    /* cycle through keys */
    uint32_t j = 0;
    for ( uint32_t i = 0; ; ) {
      if ( this->keys[ i ]->status == MEMCACHED_CONTINUE ||
           this->keys[ i ]->status == MEMCACHED_DEPENDS ) {
        if ( this->exec_key_continue( *this->keys[ i ] ) == MEMCACHED_SUCCESS )
          break;
        j = 0;
      }
      else if ( ++j == this->key_cnt )
        break;
      if ( ++i == this->key_cnt )
        i = 0;
    }
  }
}

MemcachedStatus
MemcachedExec::exec_store( EvKeyCtx &ctx ) noexcept
{
  const char * value;
  size_t       valuelen,
               curlen = 0;
  void       * data;
  uint64_t     ns,
               cnt   = 0;
  int          flags = 0;

  this->stat.set_cnt++;
  switch ( this->msg->command() ) {
    default:         break;
    case MC_ADD:     flags = MC_MUST_NOT_EXIST;
                     break;
    case MC_CAS:     flags = MC_DO_CAS | MC_MUST_EXIST;
                     break;
    case MC_APPEND:
    case MC_PREPEND: flags  = MC_DO_PEND; /* FALLTHRU */
    case MC_REPLACE: flags |= MC_MUST_EXIST;
                     break;
  }
  if ( this->msg->cas != 0 ) /* if cas is used with append, prepend */
    flags |= MC_DO_CAS;

  value    = this->msg->msg;
  valuelen = this->msg->msglen;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_NO_VALUE: /* overwrite key */
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( (flags & ( MC_DO_CAS | MC_MUST_NOT_EXIST | MC_MUST_EXIST )) != 0 ) {
        static const int not_found = 1, not_stored = 2, exists = 3;
        int status = 0;
        if ( ctx.is_new() ) {
          if ( ( flags & MC_MUST_EXIST ) != 0 ) {
            if ( ( flags & MC_DO_CAS ) != 0 ) {
              this->stat.cas_miss++;
              status = not_found;
            }
            else {
              status = not_stored;
            }
          }
        }
        /* if ( ! ctx.is_new() ) */
        else if ( ( flags & MC_MUST_NOT_EXIST ) != 0 )
          status = not_stored;
        if ( status == 0 && ( flags & MC_DO_CAS ) != 0 ) {
          cnt = 1ULL + this->kctx.serial -
                ( this->kctx.key & ValueCtr::SERIAL_MASK );
          if ( cnt != this->msg->cas ) {
            this->stat.cas_badval++;
            status = exists;
          }
          else {
            this->stat.cas_hit++;
          }
        }
        if ( status != 0 ) {
          if ( ! this->msg->is_quiet() ) {
            if ( status == not_found )
              this->strm.sz += this->send_string( mc_not_found,
                                                  mc_not_found_len );
            else if ( status == not_stored )
              this->strm.sz += this->send_string( mc_not_stored,
                                                  mc_not_stored_len );
            else
              this->strm.sz += this->send_string( mc_exists,
                                                  mc_exists_len );
          }
          return MEMCACHED_OK;
        }
      }
      if ( (ns = this->msg->ttl) != 0 ) {
        ns *= (uint64_t) 1000 * 1000 * 1000;
        if ( ns < this->kctx.ht.hdr.current_stamp )
          ns += this->kctx.ht.hdr.current_stamp;
        this->kctx.update_stamps( ns, 0 );
      }
      else {
        this->kctx.clear_stamps( true, false );
      }
      if ( ( flags & MC_DO_PEND ) != 0 ) {
        ctx.kstatus = this->kctx.get_size( curlen );
        if ( ctx.kstatus == KEY_OK )
          ctx.kstatus = this->kctx.resize( &data, curlen + valuelen, true );
      }
      else {
        ctx.kstatus = this->kctx.resize( &data, valuelen );
      }
      if ( ctx.kstatus == KEY_OK ) {
        if ( ( flags & MC_DO_PEND ) != 0 ) {
          if ( this->msg->command() == MC_PREPEND ) {
            ::memmove( &((char *) data)[ valuelen ], data, curlen );
            ::memcpy( data, value, valuelen );
          }
          else { /* append */
            ::memcpy( &((char *) data)[ curlen ], value, valuelen );
          }
        }
        else { /* set / add / replace */
          ::memcpy( data, value, valuelen );
        }
        this->kctx.set_val( this->msg->flags );
        if ( ! this->msg->is_quiet() )
          this->strm.sz += this->send_string( mc_stored, mc_stored_len );
        return MEMCACHED_OK;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV;
  }
}

MemcachedStatus
MemcachedExec::exec_bin_store( EvKeyCtx &ctx ) noexcept
{
  const char * value;
  size_t       valuelen,
               curlen = 0;
  void       * data;
  uint64_t     ns,
               cnt   = 0;
  int          flags = 0;

  this->stat.set_cnt++;
  switch ( this->msg->command() ) {
    default:         break;
    case MC_ADD:     flags = MC_MUST_NOT_EXIST;
                     break;
    case MC_CAS:     flags = MC_DO_CAS | MC_MUST_EXIST;
                     break;
    case MC_APPEND:
    case MC_PREPEND: flags  = MC_DO_PEND; /* FALLTHRU */
    case MC_REPLACE: flags |= MC_MUST_EXIST;
                     break;
  }
  if ( this->msg->cas != 0 ) /* if cas is used with append, prepend */
    flags |= MC_DO_CAS;

  value    = this->msg->msg;
  valuelen = this->msg->msglen;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_NO_VALUE: /* overwrite key */
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( (flags & ( MC_DO_CAS | MC_MUST_NOT_EXIST | MC_MUST_EXIST )) != 0 ) {
        static const int not_found = 1, not_stored = 2, exists = 3;
        int status = 0;
        if ( ctx.is_new() ) {
          if ( ( flags & MC_MUST_EXIST ) != 0 ) {
            if ( ( flags & MC_DO_CAS ) != 0 ) {
              this->stat.cas_miss++;
              status = not_found;
            }
            else
              status = not_stored;
          }
        }
        /* if ( ! ctx.is_new() ) */
        else if ( ( flags & MC_MUST_NOT_EXIST ) != 0 )
          status = exists;
        if ( status == 0 && ( flags & MC_DO_CAS ) != 0 ) {
          cnt = 1ULL + this->kctx.serial -
                ( this->kctx.key & ValueCtr::SERIAL_MASK );
          if ( cnt != this->msg->cas ) {
            this->stat.cas_badval++;
            status = exists;
          }
          else {
            this->stat.cas_hit++;
          }
        }
        if ( status != 0 ) {
          if ( ! this->msg->is_quiet() ) {
            if ( status == not_found )
              this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
            else if ( status == not_stored )
              this->strm.sz += this->send_bin_status( MS_NOT_STORED );
            else
              this->strm.sz += this->send_bin_status( MS_EXISTS );
          }
          return MEMCACHED_OK;
        }
      }
      if ( ( flags & MC_DO_PEND ) == 0 ) { /* no extras for apeend/prepend */
        if ( (ns = this->msg->ttl) != 0 ) {
          ns *= (uint64_t) 1000 * 1000 * 1000;
          if ( ns < this->kctx.ht.hdr.current_stamp )
            ns += this->kctx.ht.hdr.current_stamp;
          this->kctx.update_stamps( ns, 0 );
        }
        else {
          this->kctx.clear_stamps( true, false );
        }
      }
      if ( ( flags & MC_DO_PEND ) != 0 ) {
        ctx.kstatus = this->kctx.get_size( curlen );
        if ( ctx.kstatus == KEY_OK )
          ctx.kstatus = this->kctx.resize( &data, curlen + valuelen, true );
      }
      else {
        ctx.kstatus = this->kctx.resize( &data, valuelen );
      }
      if ( ctx.kstatus == KEY_OK ) {
        if ( ( flags & MC_DO_PEND ) != 0 ) { /* append/prepend */
          if ( this->msg->command() == MC_PREPEND ) {
            ::memmove( &((char *) data)[ valuelen ], data, curlen );
            ::memcpy( data, value, valuelen );
          }
          else { /* append */
            ::memcpy( &((char *) data)[ curlen ], value, valuelen );
          }
        }
        else { /* set / add / replace */
          ::memcpy( data, value, valuelen );
        }
        if ( ( flags & MC_DO_PEND ) == 0 ) { /* no extras for apeend/prepend */
          this->kctx.set_val( this->msg->flags );
        }
        if ( ! this->msg->is_quiet() ) {
          char * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) );

          if ( buf != NULL ) {
            MemcachedBinHdr hdr;
            ::memset( &hdr, 0, sizeof( hdr ) );
            hdr.magic   = 0x81;
            hdr.opcode  = this->msg->opcode;
            hdr.opaque  = this->msg->opaque; /* could be a serial number */
            hdr.cas     = kv_bswap64( 1ULL + this->kctx.serial -
                                   ( this->kctx.key & ValueCtr::SERIAL_MASK ) );
            ::memcpy( buf, &hdr, sizeof( hdr ) );
            this->strm.sz += sizeof( hdr );
          }
        }
        return MEMCACHED_OK;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV;
  }
}

void
MemcachedExec::multi_get_send( void ) noexcept
{
  for ( uint32_t i = 0; i < this->key_cnt; i++ ) {
    EvKeyTempResult * part;
    if ( (part = this->keys[ i ]->part) != NULL ) {
      if ( part->size < 256 )
        this->strm.append( part->data( 0 ), part->size );
      else
        this->strm.append_iov( part->data( 0 ), part->size );
    }
  }
  this->strm.sz += this->send_string( mc_end, mc_end_len );
}

static inline size_t
format_value( char *str,  const char *key,  uint16_t keylen,  uint32_t flags,
              const void *data,  size_t size,  uint64_t cas,  bool is_end )
{
  /* VALUE key <flags> <len> \r\n END \r\n */
  size_t sz = mc_value_len;
  ::memcpy( str, mc_value, mc_value_len );
  str[ sz++ ] = ' ';
  ::memcpy( &str[ sz ], key, keylen );
  sz += keylen;
  str[ sz++ ] = ' ';
  sz += uint64_to_string( flags, &str[ sz ] );
  str[ sz++ ] = ' ';
  sz += uint64_to_string( size, &str[ sz ] );
  if ( cas != 0 ) {
    str[ sz++ ] = ' ';
    sz += uint64_to_string( cas, &str[ sz ] );
  }
  sz = crlf( str, sz );
  ::memcpy( &str[ sz ], data, size );
  sz += size;
  if ( is_end ) {
    sz = crlf( str, sz );
    ::memcpy( &str[ sz ], mc_end, mc_end_len );
    sz += mc_end_len;
  }
  return crlf( str, sz );
}

size_t
MemcachedExec::send_value( EvKeyCtx &ctx,  const void *data,
                           size_t size ) noexcept
{
  /* VALUE key <flags> <len> \r\n END \r\n */
  char   * key    = ctx.kbuf.u.buf;
  uint16_t keylen = ctx.kbuf.keylen - 1;
  uint32_t flags  = this->kctx.get_val();
  size_t   sz     = mc_value_len + keylen + size + 64;
  uint64_t cnt    = 0;
  if ( this->msg->command() == MC_GETS || this->msg->command() == MC_GATS )
    cnt = 1ULL + this->kctx.serial - ( this->kctx.key & ValueCtr::SERIAL_MASK );
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return 0;
  return format_value( str, key, keylen, flags, data, size, cnt, true );
}

size_t 
MemcachedExec::send_bin_value( EvKeyCtx &ctx,  const void *s,
                               size_t slen ) noexcept
{
  /* binary GET[Qk] result: flags + cas + maybe key + data */
  uint16_t keylen  = ( this->msg->wants_key() ? ctx.kbuf.keylen - 1 : 0 );
  uint32_t datalen = (uint32_t) ( slen + 4 + keylen );
  char   * buf     = this->strm.alloc( sizeof( MemcachedBinHdr ) + datalen );

  if ( buf != NULL ) {
    MemcachedBinHdr hdr;
    uint32_t flags = kv_bswap32( this->kctx.get_val() );
    ::memset( &hdr, 0, sizeof( hdr ) );
    hdr.magic    = 0x81;
    hdr.opcode   = this->msg->opcode;
    hdr.opaque   = this->msg->opaque; /* could be a serial number */
    hdr.keylen   = kv_bswap16( keylen );
    hdr.extralen = 4;
    hdr.datalen  = kv_bswap32( (uint32_t) ( slen + 4 ) );
    hdr.cas      = kv_bswap64( 1ULL + this->kctx.serial -
                                   ( this->kctx.key & ValueCtr::SERIAL_MASK ) );
    ::memcpy( buf, &hdr, sizeof( hdr ) );
    ::memcpy( &buf[ sizeof( hdr ) ], &flags, 4 );
    if ( keylen > 0 )
      ::memcpy( &buf[ sizeof( hdr ) + 4 ], ctx.kbuf.u.buf, keylen );
    ::memcpy( &buf[ sizeof( hdr ) + 4 + keylen ], s, slen );
    return sizeof( hdr ) + slen + 4 + keylen;
  }
  return 0;
}

bool
MemcachedExec::save_value( EvKeyCtx &ctx,  const void *data,  size_t size ) noexcept
{
  char   * key    = ctx.kbuf.u.buf;
  uint16_t keylen = ctx.kbuf.keylen - 1;
  uint32_t flags  = this->kctx.get_val();
  size_t   msz    = sizeof( EvKeyTempResult ) +
                    mc_value_len + keylen + size + 64;
  uint64_t cnt    = 0;
  if ( this->msg->command() == MC_GETS || this->msg->command() == MC_GATS )
    cnt = 1ULL + this->kctx.serial - ( this->kctx.key & ValueCtr::SERIAL_MASK );
  if ( ctx.part == NULL || msz > ctx.part->mem_size ) {
    EvKeyTempResult *part;
    part = (EvKeyTempResult *) this->strm.alloc_temp( msz );
    if ( part != NULL ) {
      part->mem_size = msz;
      /*part->type = 0; no type */
      ctx.part = part;
    }
    else {
      return false;
    }
  }
  char *str = ctx.part->data( 0 );
  ctx.part->size =
    format_value( str, key, keylen, flags, data, size, cnt, false );
  return true;
}

MemcachedStatus
MemcachedExec::exec_retr( EvKeyCtx &ctx ) noexcept
{
  void * data;
  size_t size;
  /* GET key */
  this->stat.get_cnt++;
  switch ( this->get_key_read( ctx, MD_STRING ) )
    case KEY_OK: {
      this->stat.get_hit++;
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        size_t sz = 0;
        if ( this->key_cnt == 1 ) /* single key case */
          sz = this->send_value( ctx, data, size );
        else if ( ! this->save_value( ctx, data, size ) ) /* multi key case */
          return MEMCACHED_ALLOC_FAIL;

        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK ) {
          this->strm.sz += sz;
          return MEMCACHED_OK;
        }
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV; /* may retry */
    case KEY_NOT_FOUND: /*NOT_EXIST*/
    case KEY_NO_VALUE:  /*BAD_TYPE*/
      this->stat.get_miss++;
      break;
  }
  if ( this->key_cnt == 1 ) {
    this->strm.sz += this->send_string( mc_end, mc_end_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_retr( EvKeyCtx &ctx ) noexcept
{
  void * data;
  size_t size;
  /* GET / GETQ / GETK / GETKQ key */
  this->stat.get_cnt++;
  switch ( this->get_key_read( ctx, MD_STRING ) )
    case KEY_OK: {
    this->stat.get_hit++;
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        size_t sz = this->send_bin_value( ctx, data, size );
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK ) {
          this->strm.sz += sz;
          return MEMCACHED_OK;
        }
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV; /* may retry */
    case KEY_NOT_FOUND:
    case KEY_NO_VALUE:
      this->stat.get_miss++;
      if ( ! this->msg->is_quiet() ) {
        if ( this->msg->wants_key() )
          this->strm.sz += this->send_bin_status_key( MS_NOT_FOUND, ctx );
        else
          this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
      }
      break;
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_retr_touch( EvKeyCtx &ctx ) noexcept
{
  void   * data;
  size_t   size;
  uint64_t ns;
  /* GAT ttl key */
  this->stat.touch_cnt++;
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
      this->stat.touch_hit++;
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        if ( (ns = this->msg->ttl) != 0 ) {
          ns *= (uint64_t) 1000 * 1000 * 1000;
          if ( ns < this->kctx.ht.hdr.current_stamp )
            ns += this->kctx.ht.hdr.current_stamp;
          this->kctx.update_stamps( ns, 0 );
        }
        else {
          this->kctx.clear_stamps( true, false );
        }
        if ( this->key_cnt == 1 ) {
          this->strm.sz += this->send_value( ctx, data, size );
          return MEMCACHED_OK;
        }
        if ( ! this->save_value( ctx, data, size ) )
          return MEMCACHED_ALLOC_FAIL;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:    /*NOT_EXIST*/
    case KEY_NO_VALUE:  /*BAD_TYPE*/
      this->stat.touch_miss++;
      break;
  }
  if ( this->key_cnt == 1 ) {
    this->strm.sz += this->send_string( mc_end, mc_end_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_retr_touch( EvKeyCtx &ctx ) noexcept
{
  void   * data;
  size_t   size;
  uint64_t ns;
  /* GAT ttl key */
  this->stat.touch_cnt++;
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
      this->stat.touch_hit++;
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        if ( (ns = this->msg->ttl) != 0 ) {
          ns *= (uint64_t) 1000 * 1000 * 1000;
          if ( ns < this->kctx.ht.hdr.current_stamp )
            ns += this->kctx.ht.hdr.current_stamp;
          this->kctx.update_stamps( ns, 0 );
        }
        else {
          this->kctx.clear_stamps( true, false );
        }
        this->strm.sz += this->send_bin_value( ctx, data, size );
        return MEMCACHED_OK;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:
    case KEY_NO_VALUE:
      this->stat.touch_miss++;
      if ( ! this->msg->is_quiet() ) {
        if ( this->msg->wants_key() )
          this->strm.sz += this->send_bin_status_key( MS_NOT_FOUND, ctx );
        else
          this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
      }
      break;
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_del( EvKeyCtx &ctx ) noexcept
{
  bool del = false;
  /* DELETE key1 [noreply] */
  if ( this->exec_key_fetch( ctx, true ) == KEY_OK ) { /* test exists */
    if ( this->exec_key_fetch( ctx, false ) == KEY_OK ) {
      this->kctx.tombstone();
      del = true;
    }
  }
  if ( del ) this->stat.delete_hit++;
  else       this->stat.delete_miss++;
  if ( ! this->msg->is_quiet() ) {
    if ( del )
      this->strm.sz += this->send_string( mc_del, mc_del_len );
    else
      this->strm.sz += this->send_string( mc_not_found, mc_not_found_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_del( EvKeyCtx &ctx ) noexcept
{
  bool del = false;
  /* DELETE key1 [noreply] */
  if ( this->exec_key_fetch( ctx, true ) == KEY_OK ) { /* test exists */
    if ( this->exec_key_fetch( ctx, false ) == KEY_OK ) {
      this->kctx.tombstone();
      del = true;
    }
  }
  if ( del ) this->stat.delete_hit++;
  else       this->stat.delete_miss++;
  if ( ! this->msg->is_quiet() ) {
    if ( del ) {
      char * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) );

      if ( buf != NULL ) {
        MemcachedBinHdr hdr;
        ::memset( &hdr, 0, sizeof( hdr ) );
        hdr.magic   = 0x81;
        hdr.opcode  = this->msg->opcode;
        hdr.opaque  = this->msg->opaque; /* could be a serial number */
        ::memcpy( buf, &hdr, sizeof( hdr ) );
        this->strm.sz += sizeof( hdr );
      }
    }
    else {
      this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
    }
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_touch( EvKeyCtx &ctx ) noexcept
{
  uint64_t ns;
  bool not_found = false;
  /* TOUCH key ttl [noreply] */
  this->stat.touch_cnt++;
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
      this->stat.touch_hit++;
      if ( (ns = this->msg->ttl) != 0 ) {
        ns *= (uint64_t) 1000 * 1000 * 1000;
        if ( ns < this->kctx.ht.hdr.current_stamp )
          ns += this->kctx.ht.hdr.current_stamp;
        this->kctx.update_stamps( ns, 0 );
      }
      else {
        this->kctx.clear_stamps( true, false );
      }
      break;
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:   /*NOT_EXIST*/
    case KEY_NO_VALUE: /*BAD_TYPE*/
      not_found = true;
      this->stat.touch_miss++;
      break;
  }
  if ( ! this->msg->is_quiet() ) {
    if ( not_found )
      this->strm.sz += this->send_string( mc_not_found, mc_not_found_len );
    else
      this->strm.sz += this->send_string( mc_touch, mc_touch_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_touch( EvKeyCtx &ctx ) noexcept
{
  uint64_t ns;
  bool not_found = false;
  /* TOUCH key ttl [noreply] */
  this->stat.touch_cnt++;
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
      this->stat.touch_hit++;
      if ( (ns = this->msg->ttl) != 0 ) {
        ns *= (uint64_t) 1000 * 1000 * 1000;
        if ( ns < this->kctx.ht.hdr.current_stamp )
          ns += this->kctx.ht.hdr.current_stamp;
        this->kctx.update_stamps( ns, 0 );
      }
      else {
        this->kctx.clear_stamps( true, false );
      }
      break;
      /* FALLTHRU */
    default:  return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:   /*NOT_EXIST*/
    case KEY_NO_VALUE: /*BAD_TYPE*/
      not_found = true;
      this->stat.touch_miss++;
      break;
  }
  if ( ! this->msg->is_quiet() ) {
    if ( not_found )
      this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
    else
      this->strm.sz += this->send_bin_value( ctx, NULL, 0 );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_incr( EvKeyCtx &ctx ) noexcept
{
  void   * data;
  char   * str;
  size_t   size,
           sz;
  uint64_t ival;

  ival = 0;
  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus != KEY_OK )
        return MEMCACHED_ERR_KV;
      if ( string_to_uint( (char *) data, size, ival ) != 0 ) {
        this->send_err( MEMCACHED_BAD_INCR );
        break;
      }
      if ( this->msg->command() == MC_INCR ) {
        this->stat.incr_hit++;
        ival += this->msg->inc;
      }
      else {
        this->stat.decr_hit++;
        if ( ival > this->msg->inc )
          ival -= this->msg->inc;
        else
          ival = 0;
      }
      str = this->strm.alloc( 32 );
      sz = uint64_to_string( ival, str );
      sz = crlf( str, sz );
      ctx.kstatus = this->kctx.resize( &data, sz - 2 );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, str, sz - 2 );
        this->strm.sz += sz;
        break;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV;
    case KEY_IS_NEW:
    case KEY_NO_VALUE:
      this->send_string( mc_not_found, mc_not_found_len );
      if ( this->msg->command() == MC_INCR )
        this->stat.incr_miss++;
      else
        this->stat.decr_miss++;
      break;
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_incr( EvKeyCtx &ctx ) noexcept
{
  void   * data;
  size_t   size,
           sz;
  char     str[ 32 ];
  uint64_t ival;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus != KEY_OK )
        return MEMCACHED_ERR_KV;
      if ( string_to_uint( (char *) data, size, ival ) != 0 ) {
        this->strm.sz += this->send_bin_status( MS_BAD_INCR_VALUE );
        break;
      }
      if ( this->msg->command() == MC_INCR ) {
        this->stat.incr_hit++;
        ival += this->msg->inc;
      }
      else {
        this->stat.decr_hit++;
        if ( ival > this->msg->inc )
          ival -= this->msg->inc;
        else
          ival = 0;
      }
      if ( 0 ) {
      /* FALLTHRU */
    case KEY_IS_NEW:
    case KEY_NO_VALUE:
        /* key is new, use ini value */
        if ( this->msg->command() == MC_INCR ) /* is it a miss? */
          this->stat.incr_miss++;
        else
          this->stat.decr_miss++;
        ival = this->msg->ini;
      }
      sz = uint64_to_string( ival, str );
      ctx.kstatus = this->kctx.resize( &data, sz );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, str, sz );
        ival = kv_bswap64( ival );

        char * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) + 8 );
        if ( buf != NULL ) {
          MemcachedBinHdr hdr;
          ::memset( &hdr, 0, sizeof( hdr ) );
          hdr.magic   = 0x81;
          hdr.opcode  = this->msg->opcode;
          hdr.opaque  = this->msg->opaque; /* could be a serial number */
          hdr.datalen = kv_bswap32( 8 );
          hdr.cas     = kv_bswap64( 1ULL + this->kctx.serial -
                                   ( this->kctx.key & ValueCtr::SERIAL_MASK ) );
          ::memcpy( buf, &hdr, sizeof( hdr ) );
          ::memcpy( &buf[ sizeof( hdr ) ], &ival, 8 );
          this->strm.sz += sizeof( hdr ) + 8;
        }
        break;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV;
  }
  return MEMCACHED_OK;
}

struct StatFmt {
  char * b;
  size_t off, sz;

  StatFmt( char *s,  size_t len ) : b( s ), off( 0 ), sz( len ) {}

  void printf( const char *fmt, ... ) {
    if ( this->sz > 20 ) {
      va_list args;
      va_start( args, fmt );
      size_t n = vsnprintf( &b[ off ], this->sz, fmt, args );
      n = min_int( n, this->sz - 1 );
      this->off += n; this->sz -= n;
      va_end( args );
    }
  }
};

void
MemcachedExec::put_stats( void ) noexcept
{
  uint64_t now = kv_current_realtime_ns();
  StatFmt fmt( this->strm.alloc( 4 * 1024 ), 4 * 1024 );
  fmt.printf( "STAT pid %u\r\n", getpid() );
  fmt.printf( "STAT uptime %" PRIu64 "\r\n",
              ( now - this->stat.boot_time ) / 1000000000 );
  fmt.printf( "STAT time %" PRIu64 "\r\n", now / 1000000000 );
  fmt.printf( "STAT version %s\r\n", kv_stringify( DS_VER ) );
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
  struct rusage usage;
  ::getrusage( RUSAGE_SELF, &usage );
  fmt.printf( "STAT rusage_user %.6f\r\n",
              (double) usage.ru_utime.tv_sec +
              (double) usage.ru_utime.tv_usec / 1000000.0 );
  fmt.printf( "STAT rusage_system %.6f\r\n",
              (double) usage.ru_stime.tv_sec +
              (double) usage.ru_stime.tv_usec / 1000000.0 );
#endif
  fmt.printf( "STAT max_connections %u\r\n", this->stat.max_connections );
  fmt.printf( "STAT curr_connections %u\r\n", this->stat.curr_connections );
  fmt.printf( "STAT total_connections %u\r\n", this->stat.total_connections );
  fmt.printf( "STAT rejected_connections 0\r\n" );
  fmt.printf( "STAT connection_structures %u\r\n", this->stat.conn_structs );
  fmt.printf( "STAT reserved_fds 0\r\n" );

  fmt.printf( "STAT cmd_get %" PRIu64 "\r\n", this->stat.get_cnt );
  fmt.printf( "STAT cmd_set %" PRIu64 "\r\n", this->stat.set_cnt );
  fmt.printf( "STAT cmd_flush %" PRIu64 "\r\n", this->stat.flush_cnt );
  fmt.printf( "STAT cmd_touch %" PRIu64 "\r\n", this->stat.touch_cnt );
  fmt.printf( "STAT get_hits %" PRIu64 "\r\n", this->stat.get_hit );
  fmt.printf( "STAT get_misses %" PRIu64 "\r\n", this->stat.get_miss );
  fmt.printf( "STAT get_expired %" PRIu64 "\r\n", this->stat.get_expired );
  fmt.printf( "STAT get_flushed %" PRIu64 "\r\n", this->stat.get_flushed );
  fmt.printf( "STAT delete_misses %" PRIu64 "\r\n", this->stat.delete_miss );
  fmt.printf( "STAT delete_hits %" PRIu64 "\r\n", this->stat.delete_hit );
  fmt.printf( "STAT incr_misses %" PRIu64 "\r\n", this->stat.incr_miss );
  fmt.printf( "STAT incr_hits %" PRIu64 "\r\n", this->stat.incr_hit );
  fmt.printf( "STAT decr_misses %" PRIu64 "\r\n", this->stat.decr_miss );
  fmt.printf( "STAT decr_hits %" PRIu64 "\r\n", this->stat.decr_hit );
  fmt.printf( "STAT cas_misses %" PRIu64 "\r\n", this->stat.cas_miss );
  fmt.printf( "STAT cas_hits %" PRIu64 "\r\n", this->stat.cas_miss );
  fmt.printf( "STAT cas_badval %" PRIu64 "\r\n", this->stat.cas_badval );
  fmt.printf( "STAT touch_hits %" PRIu64 "\r\n", this->stat.touch_hit );
  fmt.printf( "STAT touch_misses %" PRIu64 "\r\n", this->stat.touch_miss );
  fmt.printf( "STAT auth_cmds %" PRIu64 "\r\n", this->stat.auth_cmds );
  fmt.printf( "STAT auth_errors %" PRIu64 "\r\n", this->stat.auth_errors );
  fmt.printf( "STAT bytes_read %" PRIu64 "\r\n", this->stat.bytes_read );
  fmt.printf( "STAT bytes_written %" PRIu64 "\r\n", this->stat.bytes_written );
  fmt.printf( "STAT limit_maxbytes %" PRIu64 "\r\n", this->kctx.ht.hdr.map_size );

  this->strm.sz += fmt.off;
}

void
MemcachedExec::put_stats_settings( void ) noexcept
{
  StatFmt fmt( this->strm.alloc( 4 * 1024 ), 4 * 1024 );

  fmt.printf( "STAT maxbytes %" PRIu64 "\r\n", this->kctx.ht.hdr.map_size );
  fmt.printf( "STAT maxconns %u\r\n", this->stat.max_connections );
  fmt.printf( "STAT tcpport %u\r\n", this->stat.tcpport );
  fmt.printf( "STAT udpport %u\r\n", this->stat.udpport );
  fmt.printf( "STAT inter %s\r\n",
              this->stat.interface[ 0 ] != '\0' ? this->stat.interface : "*" );
  fmt.printf( "STAT evictions on\r\n" );

  this->strm.sz += fmt.off;
}

void
MemcachedExec::put_stats_items( void ) noexcept
{
/*
STAT items:12:number 311074
STAT items:12:number_hot 0
STAT items:12:number_warm 124429
STAT items:12:number_cold 186645
 */
}

void
MemcachedExec::put_stats_sizes( void ) noexcept
{
/*
histogram of sizes
 */
}

void
MemcachedExec::put_stats_slabs( void ) noexcept
{
/*
STAT 12:chunk_size 1184
STAT 12:chunks_per_page 885
STAT 12:total_pages 352
STAT 12:total_chunks 311520
STAT 12:used_chunks 311074
STAT 12:free_chunks 446
STAT 12:free_chunks_end 0
STAT 12:mem_requested 343114622
STAT 12:get_hits 2799600
STAT 12:cmd_set 311074
STAT 12:delete_hits 0
STAT 12:incr_hits 0
STAT 12:decr_hits 0
STAT 12:cas_hits 0
STAT 12:cas_badval 0
STAT 12:touch_hits 0
STAT active_slabs 1
STAT total_malloced 369098752
 */
}

void
MemcachedExec::put_stats_conns( void ) noexcept
{
/*
STAT 28:addr tcp:127.0.0.1:11211
STAT 28:state conn_listening
STAT 28:secs_since_last_cmd 268101
STAT 29:addr tcp6:[::1]:11211
STAT 29:state conn_listening
STAT 29:secs_since_last_cmd 268101
STAT 30:addr udp:127.0.0.1:11211
STAT 30:state conn_read
STAT 30:secs_since_last_cmd 268087
STAT 31:addr udp6:[::1]:11211
STAT 31:state conn_read
STAT 31:secs_since_last_cmd 268101
*/
}

bool
MemcachedExec::do_slabs( void ) noexcept
{
/*
slabs reassign <source class> <dest class>
 */
  return true;
}
bool
MemcachedExec::do_lru( void ) noexcept
{
/*
lru <tune|mode|temp_ttl> <option list>
 */
  return true;
}
bool
MemcachedExec::do_lru_crawler( void ) noexcept
{
/*
lru_crawler <enable|disable>
lru_crawler sleep <microseconds>
lru_crawler tocrawl <32u>
 */
  return true;
}
bool
MemcachedExec::do_watch( void ) noexcept
{
/*
watch <fetchers|mutations|evictions>
 */
  return true;
}
bool
MemcachedExec::do_flush_all( void ) noexcept
{
/*
flush_all [delay_secs]
 */
  return true;
}
bool
MemcachedExec::do_memlimit( void ) noexcept
{
/*
cache_memlimit  <nbytes>
 */
  return true;
}
void
MemcachedExec::do_no_op( void ) noexcept
{
  static const uint8_t no_op[ 24 ] = { 0x80, 0x0a };
  this->strm.append( no_op, 24 );
}
