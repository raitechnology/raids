#ifndef __rai_raids__redis_msg_h__
#define __rai_raids__redis_msg_h__

#include <raikv/util.h>
#include <raikv/work.h>

namespace rai {
namespace ds {

struct BulkStr;
struct BulkArr;

enum RedisMsgStatus {
  REDIS_MSG_OK         = 0,
  REDIS_MSG_BAD_TYPE   = 1,
  REDIS_MSG_PARTIAL    = 2,
  REDIS_MSG_ALLOC_FAIL = 3,
  REDIS_MSG_BAD_JSON   = 4
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
  int match_arg( int n,  const char *str,  size_t sz,  ... );

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
  void set_simple_string( char *s,  size_t sz = 0 ) {
    this->type   = SIMPLE_STRING;
    this->len    = ( sz == 0 ? ::strlen( s ) : sz );
    this->strval = s;
  }
  void set_int( int64_t i ) {
    this->type = INTEGER_VALUE;
    this->len  = 0;
    this->ival = i;
  }
  bool alloc_array( kv::ScratchMem &wrk,  int64_t sz );

  RedisMsgStatus pack( void *buf,  size_t &len );
  RedisMsgStatus split( kv::ScratchMem &wrk );
  RedisMsgStatus unpack( void *buf,  size_t &len,  kv::ScratchMem &wrk );
  RedisMsgStatus to_json( char *buf,  size_t &len ) const;
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
