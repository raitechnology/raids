#ifndef __rai_raids__redis_msg_h__
#define __rai_raids__redis_msg_h__

#include <raikv/util.h>
#include <raikv/work.h>

namespace rai {
namespace ds {

struct BulkStr;
struct BulkArr;

enum RedisMsgStatus {
  REDIS_MSG_OK           = 0,
  REDIS_MSG_BAD_TYPE     = 1,
  REDIS_MSG_PARTIAL      = 2,
  REDIS_MSG_ALLOC_FAIL   = 3,
  REDIS_MSG_BAD_JSON     = 4,
  REDIS_MSG_BAD_INT      = 5,
  REDIS_MSG_INT_OVERFLOW = 6
};

const char *redis_msg_status_string( RedisMsgStatus status );
const char *redis_msg_status_description( RedisMsgStatus status );

struct JsonInput;

struct RedisMsg {
  enum DataType {
    SIMPLE_STRING = '+', /* 43 */
    ERROR_STRING  = '-', /* 45 */
    INTEGER_VALUE = ':', /* 58 */
    BULK_STRING   = '$', /* 36 */
    BULK_ARRAY    = '*'  /* 42 */
  };

  DataType type;
  int64_t  len;   /* size of string or array, no value for ints */

  union {
    char     * strval;   /* simple, bulk string */
    int64_t    ival;     /* integer */
    RedisMsg * array;    /* bulk array */
  };

#define DTBit( t ) ( 1U << ( (uint8_t) t - (uint8_t) RedisMsg::BULK_STRING ) )
  bool is_string( void ) const {
    static const uint32_t valid_bits = DTBit( SIMPLE_STRING ) |
                                       DTBit( BULK_STRING );
    return ( DTBit( this->type ) & valid_bits ) != 0;
  }

  /* get the first string in an array: ["command"] */
  const char * command( size_t &length,  size_t &argc ) const {
    const RedisMsg *m = this;
    if ( m->type == BULK_ARRAY ) {
      if ( m->len > 0 ) {
        argc = (size_t) m->len;
        m = &m->array[ 0 ];
      }
      else
        goto no_cmd;
    }
    else {
      argc = 1;
    }
    if ( m->is_string() ) {
      length = m->len;
      return m->strval;
    }
  no_cmd:
    length = 0;
    argc = 0;
    return NULL;
  }
  const char * command( size_t &length ) const {
    size_t tmp;
    return this->command( length, tmp );
  }
  /* get a string argument and length */
  bool get_arg( int n,  const char *&str,  size_t &sz ) const {
    if ( n < this->len && this->array[ n ].is_string() ) {
      if ( this->array[ n ].len >= 0 ) {
        str = this->array[ n ].strval;
        sz  = this->array[ n ].len;
        return true;
      }
    }
    return false;
  }
  /* get an integer argument */
  bool get_arg( int n,  int64_t &i ) const {
    if ( n < this->len ) {
      if ( this->array[ n ].is_string() ) {
        if ( this->array[ n ].len > 0 ) {
          const char * str = this->array[ n ].strval;
          size_t       sz  = this->array[ n ].len;
          return str_to_int( str, sz, i ) == REDIS_MSG_OK;
        }
      }
      else if ( this->array[ n ].type == INTEGER_VALUE ) {
        i = this->array[ n ].ival;
        return true;
      }
    }
    return false;
  }
  /* match argument by string, returns which arg matched, 0 if none
   * the arg n indicates which arg to start, -2 means args [2 -> end]
   *   int n = match( -2, "one", "two", NULL );
   *   if ( n == 1 ) matched one
   *   if ( n == 2 ) matched two
   *   if ( n == 0 ) matched none */
  int match_arg( int n,  const char *str,  size_t sz,  ... );

  /* integer to string routines */
  static uint64_t neg( int64_t v ) {
    if ( (uint64_t) v == ( (uint64_t) 1 << 63 ) )
      return ( (uint64_t) 1 << 63 );
    return (uint64_t) -v;
  }
  static size_t uint_digits( uint64_t v ) {
    for ( size_t n = 1; ; n += 4 ) {
      if ( v < 10 )    return n;
      if ( v < 100 )   return n + 1;
      if ( v < 1000 )  return n + 2;
      if ( v < 10000 ) return n + 3;
      v /= 10000;
    }
  }
  static size_t int_digits( int64_t v ) {
    if ( v < 0 ) return 1 + uint_digits( neg( v ) );
    return uint_digits( v );
  }
  /* does not null terminate (most strings have lengths, not nulls) */
  static size_t uint_to_str( uint64_t v,  char *buf,  size_t len ) {
    for ( size_t pos = len; v >= 10; ) {
      const uint64_t q = v / 10, r = v % 10;
      buf[ --pos ] = '0' + r;
      v = q;
    }
    buf[ 0 ] = '0' + v;
    return len;
  }
  static size_t uint_to_str( uint64_t v,  char *buf ) {
    return uint_to_str( v, buf, uint_digits( v ) );
  }
  static size_t int_to_str( int64_t v,  char *buf,  size_t len ) {
    if ( v < 0 ) {
      buf[ 0 ] = '-';
      return 1 + uint_to_str( neg( v ), &buf[ 1 ], len - 1 );
    }
    return uint_to_str( v, buf, len );
  }
  static size_t int_to_str( int64_t v,  char *buf ) {
    return int_to_str( v, buf, int_digits( v ) );
  }
  /* str length sz to int */
  static RedisMsgStatus str_to_int( const char *str,  size_t sz,
                                    int64_t &ival );
  /* various simple encodings used by redis */
  void set_nil( void ) {
    this->type   = BULK_STRING;
    this->len    = -1;
    this->strval = NULL;
  }
  void set_null( void ) {
    this->type  = BULK_ARRAY;
    this->len   = -1;
    this->array = NULL;
  }
  void set_mt_array( void ) {
    this->type  = BULK_ARRAY;
    this->len   = 0;
    this->array = NULL;
  }
  void set_simple_string( char *s,  size_t sz = 0 ) {
    this->type   = SIMPLE_STRING;
    this->len    = ( sz == 0 ? ::strlen( s ) : sz );
    this->strval = s;
  }
  void set_bulk_string( char *s,  size_t sz = 0 ) {
    this->type   = BULK_STRING;
    this->len    = ( sz == 0 ? ::strlen( s ) : sz );
    this->strval = s;
  }
  void set_int( int64_t i ) {
    this->type = INTEGER_VALUE;
    this->len  = 0;
    this->ival = i;
  }
  bool alloc_array( kv::ScratchMem &wrk,  int64_t sz );
  bool string_array( kv::ScratchMem &wrk,  int64_t sz,  ... );

  size_t pack_size( void ) const; /* pack() buf length */
  size_t pack( void *buf ) const;
  RedisMsgStatus pack2( void *buf,  size_t &len ) const; /* len is size */
  RedisMsgStatus split( kv::ScratchMem &wrk ); /* splits cmd line into array */
  /* try to decode one message, length of data decoded is returned in len */
  RedisMsgStatus unpack( void *buf,  size_t &len,  kv::ScratchMem &wrk );
  /* similar to pack() and pack_size(), except in json format */
  size_t to_almost_json_size( bool be_weird = true ) const;
  size_t to_almost_json( char *buf,  bool be_weird = true ) const;

  /* decode json into msg */
  RedisMsgStatus unpack_json( const char *json,  kv::ScratchMem &wrk ) {
    size_t len = ::strlen( json );
    return this->unpack_json( json, len, wrk );
  }
  RedisMsgStatus unpack_json( const char *json,  size_t &len,
                              kv::ScratchMem &wrk );
  /* internal psuedo-json processing */
  RedisMsgStatus parse_json( JsonInput &input );
  RedisMsgStatus parse_object( JsonInput &input );
  RedisMsgStatus parse_array( JsonInput &input );
  RedisMsgStatus parse_string( JsonInput &input );
  RedisMsgStatus parse_number( JsonInput &input );
};

}
}
#endif
