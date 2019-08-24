#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/time.h>
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

enum {
  /* add = must not exist, replace = must exist */
  MC_MUST_NOT_EXIST = 1,
  MC_MUST_EXIST     = 2,
  MC_DO_PEND        = 4,
  MC_DO_CAS         = 8
};

MemcachedStatus
MemcachedExec::unpack( void *buf,  size_t &buflen )
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
rai::ds::memcached_cmd_string( uint8_t cmd )
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
MemcachedMsg::print( void )
{
  uint32_t i, j;
  printf( "%s", memcached_cmd_string( this->cmd ) );
  switch ( this->command() ) {
    case MC_SET:
    case MC_ADD:
    case MC_APPEND:
    case MC_REPLACE:
    case MC_PREPEND: /* SET key flags ttl msglen */
      printf( " %.*s %u %lu %lu",
              (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->flags, this->ttl, this->msglen );
      break;
    case MC_CAS:    /* CAS key flags ttl msglen cas-id */
      printf( " %.*s %u %lu %lu %lu",
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
      printf( " %lu", this->ttl );
      i = this->first;
      for ( j = 0; j < this->keycnt; j++, i++ )
        printf( " %.*s", (int) this->args[ i ].len, this->args[ i ].str );
      break;
    case MC_DELETE: /* DELETE key */
      printf( " %.*s", (int) this->args[ 0 ].len, this->args[ 0 ].str );
      break;
    case MC_TOUCH:  /* TOUCH key ttl */
      printf( " %.*s %lu", (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->ttl );
      break;
    case MC_DECR:
    case MC_INCR:   /* INC key val */
      printf( " %.*s %lu", (int) this->args[ 0 ].len, this->args[ 0 ].str,
              this->inc );
      break;
    default:
      break;
  }
  if ( ! this->is_quiet() == 0 )
    printf( " noreply" );
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

MemcachedStatus
MemcachedMsg::unpack( void *buf,  size_t &buflen,  ScratchMem &wrk )
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
  uint32_t kw = MC_KW( s[ 0 ], s[ 1 ], s[ 2 ], s[ 3 ] );
  
  size_t          incr = 1,
                  cnt  = 0;
  MemcachedArgs * tmp  = &this->xarg;
  if ( tmp == NULL )
    return MEMCACHED_ALLOC_FAIL;

  while ( ptr < end && *ptr == ' ' )
    ptr++;
  if ( ptr < end ) {
    tmp[ 0 ].str = ptr;
    for (;;) {
      if ( ++ptr == end || *ptr == ' ' ) {
        tmp[ cnt ].len = ptr - tmp[ cnt ].str;
        cnt++;
        while ( ptr < end && *ptr == ' ' )
          ptr++; 
        if ( ptr == end )
          goto break_loop;
        if ( cnt == incr ) {
          incr += ( ( incr == 1 ) ? ( ( ( j / 8 ) | 3 ) + 1 ) : 16 );
          MemcachedArgs * tmp2 = (MemcachedArgs *)
            wrk.alloc( sizeof( MemcachedArgs ) * incr );
          if ( tmp2 == NULL )
            return MEMCACHED_ALLOC_FAIL;
          ::memcpy( tmp2, tmp, sizeof( MemcachedArgs ) * cnt );
          tmp = tmp2;
        }
        tmp[ cnt ].str = ptr;
      }
    }
  }
break_loop:;
  this->args   = tmp;
  this->argcnt = cnt;
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
    case MC_KW_VERSION: this->cmd = MC_VERSION;   return MEMCACHED_OK;
    case MC_KW_QUIT:    this->cmd = MC_QUIT;      return MEMCACHED_OK;
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
MemcachedMsg::parse_store( void )
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
MemcachedMsg::parse_cas( void )
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
MemcachedMsg::parse_retr( void )
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
MemcachedMsg::parse_gat( void )
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
MemcachedMsg::parse_del( void )
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
MemcachedMsg::parse_incr( void )
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
MemcachedMsg::parse_touch( void )
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
MemcachedMsg::parse_bin_store( MemcachedBinHdr &b,  char *ptr,  size_t &buflen )
{
  /* SET, ADD, REPLACE:  has key, has extra flags+ttl, may have msg */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
  this->flags    = __builtin_bswap32( ex[ 0 ] );
  this->ttl      = __builtin_bswap32( ex[ 1 ] );
  this->cas      = __builtin_bswap64( b.cas );
  this->msglen   = datalen - ( exlen + keylen );
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = &ptr[ keylen ]; /* the msg data */
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_pend( MemcachedBinHdr &b,  char *ptr,  size_t &buflen )
{
  /* APPEND, PREPEND:  has key, has msg */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
MemcachedMsg::parse_bin_retr( MemcachedBinHdr &b,  char *ptr,  size_t &buflen )
{
  /* DELETE, GET, GETQ, GETK, GETKQ:  has key */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
MemcachedMsg::parse_bin_touch( MemcachedBinHdr &b,  char *ptr,  size_t &buflen )
{
  /* TOUCH, GAT, GATQ:  has key, has extra ttl */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
  this->ttl      = __builtin_bswap32( ex[ 0 ] );
  this->cas      = 0;
  this->msglen   = 0;
  this->inc      = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_incr( MemcachedBinHdr &b,  char *ptr,  size_t &buflen )
{
  /* INCR, DECR:  has key, has extra inc+ini+ttl */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
  this->inc = __builtin_bswap64( this->inc );
  ::memcpy( &this->ini, &ptr[ 8 ], sizeof( this->ini ) );
  this->ini = __builtin_bswap64( this->ini );/* initial value */
  ::memcpy( ex, &ptr[ 16 ], sizeof( ex ) );
  this->ttl = __builtin_bswap32( ex[ 0 ] );
  ptr = &ptr[ 20 ];
  this->msglen   = 0;
  this->xarg.str = ptr; /* the key */
  this->xarg.len = keylen;  /* key length */
  this->msg      = NULL;
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedMsg::parse_bin_op( MemcachedBinHdr &b,  char *ptr,  size_t &buflen,
                            size_t extra_sz )
{
  /* QUIT, VERSION:  no key, may have extra */
  size_t keylen  = __builtin_bswap16( b.keylen ),
         datalen = __builtin_bswap32( b.datalen ),
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
    this->ttl = __builtin_bswap32( ex[ 0 ] );
  }
  else {
    this->ttl = 0;
  }
  this->msglen   = 0;
  this->msg      = NULL;
  return MEMCACHED_OK;
}

void
MemcachedExec::send_err( int status,  KeyStatus kstatus )
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
MemcachedExec::send_string( const void *s )
{
  return this->send_string( s, ::strlen( (const char *) s ) );
}

size_t 
MemcachedExec::send_string( const void *s,  size_t slen )
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
MemcachedExec::send_bin_status( uint16_t status,  const void *s,  size_t slen )
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
    hdr.status  = __builtin_bswap16( status );
    hdr.datalen = __builtin_bswap32( slen );
    ::memcpy( buf, &hdr, sizeof( hdr ) );
    ::memcpy( &buf[ sizeof( hdr ) ], s, slen );
    return sizeof( hdr ) + slen;
  }
  return 0;
}

size_t 
MemcachedExec::send_bin_status_key( uint16_t status,  EvKeyCtx &ctx )
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
    hdr.status  = __builtin_bswap16( status );
    hdr.keylen  = __builtin_bswap16( keylen );
    hdr.datalen = __builtin_bswap32( keylen );
    ::memcpy( buf, &hdr, sizeof( hdr ) );
    ::memcpy( &buf[ sizeof( hdr ) ], key, keylen );
    return sizeof( hdr ) + keylen;
  }
  return 0;
}

size_t
MemcachedExec::send_err_kv( KeyStatus kstatus )
{
  size_t bsz = 256;
  char * buf = this->strm.alloc( bsz );

  if ( buf != NULL ) {
    bsz = ::snprintf( buf, bsz, "SERVER_ERROR KeyCtx %d/%s %s\r\n",
                     kstatus, kv_key_status_string( (KeyStatus) kstatus ),
                     kv_key_status_description( (KeyStatus) kstatus ) );
    return bsz;
  }
  return 0;
}

MemcachedStatus
MemcachedExec::exec_key_setup( EvSocket *own,  EvPrefetchQueue *q,
                               EvKeyCtx *&ctx,  uint32_t n )
{
  const char * key    = this->msg->args[ n ].str;
  size_t       keylen = this->msg->args[ n ].len;
  void *p = this->strm.alloc_temp( EvKeyCtx::size( keylen ) );
  if ( p == NULL )
    return MEMCACHED_ALLOC_FAIL;
  ctx = new ( p ) EvKeyCtx( this->kctx.ht, own, key, keylen, n,
                            this->seed, this->seed2 );
  if ( q != NULL && ! q->push( ctx ) )
    return MEMCACHED_ALLOC_FAIL;
  ctx->status = MEMCACHED_CONTINUE;
  return MEMCACHED_SETUP_OK;
}

MemcachedStatus
MemcachedExec::exec( EvSocket *svc,  EvPrefetchQueue *q )
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
    this->keys = NULL;
    /* setup first key */
    status = this->exec_key_setup( svc, q, this->key, i );
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
            status = this->exec_key_setup( svc, q,
                                           this->keys[ this->key_cnt++ ], ++i );
          } while ( status == MEMCACHED_SETUP_OK && this->key_cnt  < cnt );
        }
      }
    }
    return status; /* cmds with keys return setup ok */
  }
  switch ( this->msg->command() ) {
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
}

kv::KeyStatus
MemcachedExec::exec_key_fetch( EvKeyCtx &ctx,  bool force_read )
{
  if ( test_read_only( this->msg->cmd ) || force_read ) {
    ctx.kstatus = this->kctx.find( &this->wrk );
    ctx.is_read = true;
  }
  else if ( test_mutator( this->msg->cmd ) != 0 ) {
    ctx.kstatus = this->kctx.acquire( &this->wrk );
    ctx.is_new = ( ctx.kstatus == KEY_IS_NEW );
    ctx.is_read = false;
  }
  else {
    ctx.kstatus = KEY_NO_VALUE;
    ctx.status  = MEMCACHED_BAD_CMD;
    ctx.is_read = true;
  }
  if ( ctx.kstatus == KEY_OK ) /* not new and is found */
    ctx.type = this->kctx.get_type();
  return ctx.kstatus;
}

MemcachedStatus
MemcachedExec::exec_key_continue( EvKeyCtx &ctx )
{
  if ( this->msg->pad != 0xaa ) {
    ctx.status = MEMCACHED_BAD_PAD;
    goto skip_key;
  }
  if ( ctx.status != MEMCACHED_CONTINUE && ctx.status != MEMCACHED_DEPENDS ) {
    if ( ++this->key_done < this->key_cnt )
      return MEMCACHED_CONTINUE;
    return MEMCACHED_SUCCESS;
  }
  if ( this->kctx.kbuf != &ctx.kbuf ||
       this->kctx.key != ctx.hash1 || this->kctx.key2 != ctx.hash2 )
    this->exec_key_prefetch( ctx );
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
    if ( ! ctx.is_read ) {
      if ( ctx.is_new && ctx.status <= MEMCACHED_SUCCESS ) {
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
MemcachedExec::exec_run_to_completion( void )
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
MemcachedExec::exec_store( EvKeyCtx &ctx )
{
  const char * value;
  size_t       valuelen,
               curlen = 0;
  void       * data;
  uint64_t     ns,
               cnt   = 0;
  int          flags = 0;

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
      ctx.is_new = true;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( (flags & ( MC_DO_CAS | MC_MUST_NOT_EXIST | MC_MUST_EXIST )) != 0 ) {
        static const int not_found = 1, not_stored = 2, exists = 3;
        int status = 0;
        if ( ctx.is_new ) {
          if ( ( flags & MC_MUST_EXIST ) != 0 ) {
            if ( ( flags & MC_DO_CAS ) != 0 )
              status = not_found;
            else
              status = not_stored;
          }
        }
        /* if ( ! ctx.is_new ) */
        else if ( ( flags & MC_MUST_NOT_EXIST ) != 0 )
          status = not_stored;
        if ( status == 0 && ( flags & MC_DO_CAS ) != 0 ) {
          cnt = 1ULL + this->kctx.serial -
                ( this->kctx.key & ValueCtr::SERIAL_MASK );
          if ( cnt != this->msg->cas )
            status = exists;
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
MemcachedExec::exec_bin_store( EvKeyCtx &ctx )
{
  const char * value;
  size_t       valuelen,
               curlen = 0;
  void       * data;
  uint64_t     ns,
               cnt   = 0;
  int          flags = 0;

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
      ctx.is_new = true;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( (flags & ( MC_DO_CAS | MC_MUST_NOT_EXIST | MC_MUST_EXIST )) != 0 ) {
        static const int not_found = 1, not_stored = 2, exists = 3;
        int status = 0;
        if ( ctx.is_new ) {
          if ( ( flags & MC_MUST_EXIST ) != 0 ) {
            if ( ( flags & MC_DO_CAS ) != 0 )
              status = not_found;
            else
              status = not_stored;
          }
        }
        /* if ( ! ctx.is_new ) */
        else if ( ( flags & MC_MUST_NOT_EXIST ) != 0 )
          status = exists;
        if ( status == 0 && ( flags & MC_DO_CAS ) != 0 ) {
          cnt = 1ULL + this->kctx.serial -
                ( this->kctx.key & ValueCtr::SERIAL_MASK );
          if ( cnt != this->msg->cas )
            status = exists;
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
            hdr.cas     = __builtin_bswap64( 1ULL + this->kctx.serial -
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
MemcachedExec::multi_get_send( void )
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
  sz += uint_to_str( flags, &str[ sz ] );
  str[ sz++ ] = ' ';
  sz += uint_to_str( size, &str[ sz ] );
  if ( cas != 0 ) {
    str[ sz++ ] = ' ';
    sz += uint_to_str( cas, &str[ sz ] );
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
MemcachedExec::send_value( EvKeyCtx &ctx,  const void *data,  size_t size )
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
MemcachedExec::send_bin_value( EvKeyCtx &ctx,  const void *s,  size_t slen )
{
  /* binary GET[Qk] result: flags + cas + maybe key + data */
  uint16_t keylen  = ( this->msg->wants_key() ? ctx.kbuf.keylen - 1 : 0 );
  uint32_t datalen = slen + 4 + keylen;
  char   * buf     = this->strm.alloc( sizeof( MemcachedBinHdr ) + datalen );

  if ( buf != NULL ) {
    MemcachedBinHdr hdr;
    uint32_t flags = __builtin_bswap32( this->kctx.get_val() );
    ::memset( &hdr, 0, sizeof( hdr ) );
    hdr.magic    = 0x81;
    hdr.opcode   = this->msg->opcode;
    hdr.opaque   = this->msg->opaque; /* could be a serial number */
    hdr.keylen   = __builtin_bswap16( keylen );
    hdr.extralen = 4;
    hdr.datalen  = __builtin_bswap32( slen + 4 );
    hdr.cas      = __builtin_bswap64( 1ULL + this->kctx.serial -
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
MemcachedExec::save_value( EvKeyCtx &ctx,  const void *data,  size_t size )
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
      part->type = 0; /* no type */
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
MemcachedExec::exec_retr( EvKeyCtx &ctx )
{
  void * data;
  size_t size;
  /* GET key */
  switch ( this->get_key_read( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:            return MEMCACHED_ERR_KV; /* may retry */
    case KEY_NOT_FOUND: /*NOT_EXIST*/ break;
    case KEY_NO_VALUE:  /*BAD_TYPE*/ break;
  }
  if ( this->key_cnt == 1 ) {
    this->strm.sz += this->send_string( mc_end, mc_end_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_retr( EvKeyCtx &ctx )
{
  void * data;
  size_t size;
  /* GET / GETQ / GETK / GETKQ key */
  switch ( this->get_key_read( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:
      return MEMCACHED_ERR_KV; /* may retry */
    case KEY_NOT_FOUND:
    case KEY_NO_VALUE:
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
MemcachedExec::exec_retr_touch( EvKeyCtx &ctx )
{
  void   * data;
  size_t   size;
  uint64_t ns;
  /* GAT ttl key */
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:            return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:    /*NOT_EXIST*/ break;
    case KEY_NO_VALUE:  /*BAD_TYPE*/ break;
  }
  if ( this->key_cnt == 1 ) {
    this->strm.sz += this->send_string( mc_end, mc_end_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_retr_touch( EvKeyCtx &ctx )
{
  void   * data;
  size_t   size;
  uint64_t ns;
  /* GAT ttl key */
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:
      return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:
    case KEY_NO_VALUE:
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
MemcachedExec::exec_del( EvKeyCtx &ctx )
{
  bool del = false;
  /* DELETE key1 [noreply] */
  if ( this->exec_key_fetch( ctx, true ) == KEY_OK ) { /* test exists */
    if ( this->exec_key_fetch( ctx, false ) == KEY_OK ) {
      this->kctx.tombstone();
      del = true;
    }
  }
  if ( ! this->msg->is_quiet() ) {
    if ( del )
      this->strm.sz += this->send_string( mc_del, mc_del_len );
    else
      this->strm.sz += this->send_string( mc_not_found, mc_not_found_len );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_del( EvKeyCtx &ctx )
{
  bool del = false;
  /* DELETE key1 [noreply] */
  if ( this->exec_key_fetch( ctx, true ) == KEY_OK ) { /* test exists */
    if ( this->exec_key_fetch( ctx, false ) == KEY_OK ) {
      this->kctx.tombstone();
      del = true;
    }
  }
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
    else
      this->strm.sz += this->send_bin_status( MS_NOT_FOUND );
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_touch( EvKeyCtx &ctx )
{
  uint64_t ns;
  bool not_found = false;
  /* TOUCH key ttl [noreply] */
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:            return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:    not_found = true; /*NOT_EXIST*/ break;
    case KEY_NO_VALUE:  not_found = true; /*BAD_TYPE*/ break;
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
MemcachedExec::exec_bin_touch( EvKeyCtx &ctx )
{
  uint64_t ns;
  bool not_found = false;
  /* TOUCH key ttl [noreply] */
  switch ( this->get_key_write( ctx, MD_STRING ) )
    case KEY_OK: {
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
    default:            return MEMCACHED_ERR_KV; /* may retry */
    case KEY_IS_NEW:    not_found = true; /*NOT_EXIST*/ break;
    case KEY_NO_VALUE:  not_found = true; /*BAD_TYPE*/ break;
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
MemcachedExec::exec_incr( EvKeyCtx &ctx )
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
      if ( size > 0 ) {
        string_to_uint( (char *) data, size, ival );
      }
      /* FALLTHRU */
    case KEY_IS_NEW:
    case KEY_NO_VALUE:
      if ( this->msg->command() == MC_INCR )
        ival += this->msg->inc;
      else {
        if ( ival > this->msg->inc )
          ival -= this->msg->inc;
        else
          ival = 0;
      }
      str = this->strm.alloc( 32 );
      sz = uint_to_str( ival, str );
      sz = crlf( str, sz );
      ctx.kstatus = this->kctx.resize( &data, sz - 2 );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, str, sz - 2 );
        this->strm.sz += sz;
        break;
      }
      /* FALLTHRU */
    default: return MEMCACHED_ERR_KV;
  }
  return MEMCACHED_OK;
}

MemcachedStatus
MemcachedExec::exec_bin_incr( EvKeyCtx &ctx )
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
      if ( size == 0 || string_to_uint( (char *) data, size, ival ) != 0 ) {
        this->strm.sz += this->send_bin_status( MS_BAD_INCR_VALUE );
        break;
      }
      if ( this->msg->command() == MC_INCR )
        ival += this->msg->inc;
      else {
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
        ival = this->msg->ini;
      }
      sz = uint_to_str( ival, str );
      ctx.kstatus = this->kctx.resize( &data, sz );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, str, sz );
        ival = __builtin_bswap64( ival );

        char * buf = this->strm.alloc( sizeof( MemcachedBinHdr ) + 8 );
        if ( buf != NULL ) {
          MemcachedBinHdr hdr;
          ::memset( &hdr, 0, sizeof( hdr ) );
          hdr.magic   = 0x81;
          hdr.opcode  = this->msg->opcode;
          hdr.opaque  = this->msg->opaque; /* could be a serial number */
          hdr.datalen = __builtin_bswap32( 8 );
          hdr.cas     = __builtin_bswap64( 1ULL + this->kctx.serial -
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
