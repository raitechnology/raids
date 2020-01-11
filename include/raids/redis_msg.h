#ifndef __rai_raids__redis_msg_h__
#define __rai_raids__redis_msg_h__

#include <raikv/util.h>
#include <raikv/work.h>
#include <raids/int_str.h>

namespace rai {
namespace ds {

struct BulkStr;
struct BulkArr;

enum RedisMsgStatus {
  REDIS_MSG_OK             = 0,
  REDIS_MSG_INT_OVERFLOW   = STR_CVT_INT_OVERFLOW,
  REDIS_MSG_BAD_INT        = STR_CVT_BAD_INT,
  REDIS_MSG_FLOAT_OVERFLOW = STR_CVT_FLOAT_OVERFLOW,
  REDIS_MSG_BAD_FLOAT      = STR_CVT_BAD_FLOAT,
  REDIS_MSG_BAD_TYPE,
  REDIS_MSG_PARTIAL,
  REDIS_MSG_ALLOC_FAIL,
  REDIS_MSG_BAD_JSON
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
  static const uint32_t string_bits = DTBit( SIMPLE_STRING ) |
                                      DTBit( BULK_STRING );
  bool is_string( void ) const {
    return ( DTBit( this->type ) & string_bits ) != 0;
  }
  static const uint32_t all_data_bits = string_bits |
                                        DTBit( ERROR_STRING ) |
                                        DTBit( INTEGER_VALUE ) |
                                        DTBit( BULK_ARRAY );
  static inline bool valid_type_char( char b ) {
    return b >= '$' && b <= '$' + 31 && ( DTBit( b ) & all_data_bits ) != 0;
  }
  /* copy msg reference */
  void ref( RedisMsg &m ) {
    this->type = m.type;
    this->len  = m.len;
    if ( m.type == INTEGER_VALUE )
      this->ival = m.ival;
    else if ( m.type == BULK_ARRAY )
      this->array = m.array;
    else
      this->strval = m.strval;
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
  /* get an double argument */
  bool get_arg( int n,  double &f ) const {
    if ( n < this->len ) {
      if ( this->array[ n ].is_string() ) {
        if ( this->array[ n ].len > 0 ) {
          const char * str = this->array[ n ].strval;
          size_t       sz  = this->array[ n ].len;
          return str_to_dbl( str, sz, f ) == REDIS_MSG_OK;
        }
      }
      else if ( this->array[ n ].type == INTEGER_VALUE ) {
        f = (double) this->array[ n ].ival;
        return true;
      }
    }
    return false;
  }
  /* match argument by string, returns which arg matched, 0 if none
   * the arg n indicates which arg to start, -2 means args [2 -> end]
   *   int n = match( -2, "one", 3, "two", 3, NULL );
   *   if ( n == 1 ) matched one
   *   if ( n == 2 ) matched two
   *   if ( n == 0 ) matched none */
  #define MARG( str ) str, sizeof( str ) - 1
  /* used as: match_arg( 1, MARG( "hello" ), MARG( "world" ), NULL ) */
  int match_arg( int n,  const char *str,  size_t sz,  ... );

  /* str length sz to int */
  static RedisMsgStatus str_to_int( const char *str,  size_t sz,
                                    int64_t &ival ) {
    return (RedisMsgStatus) rai::ds::string_to_int( str, sz, ival );
  }
  /* str length sz to uint */
  static RedisMsgStatus str_to_uint( const char *str,  size_t sz,
                                     uint64_t &ival ) {
    return (RedisMsgStatus) rai::ds::string_to_uint( str, sz, ival );
  }
  /* str length sz to double */
  static RedisMsgStatus str_to_dbl( const char *str,  size_t sz,
                                    double &fval ) {
    return (RedisMsgStatus) rai::ds::string_to_dbl( str, sz, fval );
  }
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
  /* copy message into scratch mem */
  RedisMsg *dup( kv::ScratchMem &wrk );
  RedisMsg *dup2( kv::ScratchMem &wrk,  RedisMsg &cpy );
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
