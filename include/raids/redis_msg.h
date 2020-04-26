#ifndef __rai_raids__redis_msg_h__
#define __rai_raids__redis_msg_h__

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DS_BULK_STRING   = '$', /* 36 */
  DS_BULK_ARRAY    = '*', /* 42 */
  DS_SIMPLE_STRING = '+', /* 43 */
  DS_ERROR_STRING  = '-', /* 45 */
  DS_INTEGER_VALUE = ':'  /* 58 */
} ds_resp_type_t;

typedef struct ds_msg_s ds_msg_t;

struct ds_msg_s {
  ds_resp_type_t type;
  int32_t len;  /* no value for ints, -1 nil (bulkstr) and null (bulkarr) */
  union {
    char     * strval;
    int64_t    ival;
    ds_msg_t * array;
  };
};

static inline ds_msg_t *mk_str( ds_msg_t *s,  const void *buf,  int32_t len ) {
  s->type   = DS_BULK_STRING;
  s->len    = len;
  s->strval = (char *) buf;
  return s;
}

static inline ds_msg_t *mk_int( ds_msg_t *n,  int64_t i ) {
  n->type = DS_INTEGER_VALUE;
  n->len  = 0;
  n->ival = i;
  return n;
}

static inline ds_msg_t *mk_arr( ds_msg_t *a,  int32_t len,  ds_msg_t *ar ) {
  a->type  = DS_BULK_ARRAY;
  a->len   = len;
  a->array = ar;
  return a;
}

typedef enum {
  DS_MSG_STATUS_OK = 0,
  DS_MSG_STATUS_INT_OVERFLOW,
  DS_MSG_STATUS_BAD_INT,
  DS_MSG_STATUS_FLOAT_OVERFLOW,
  DS_MSG_STATUS_BAD_FLOAT,
  DS_MSG_STATUS_BAD_TYPE,
  DS_MSG_STATUS_PARTIAL,
  DS_MSG_STATUS_ALLOC_FAIL,
  DS_MSG_STATUS_BAD_JSON
} ds_msg_status_t;

const char *ds_msg_status_string( ds_msg_status_t status );
const char *ds_msg_status_description( ds_msg_status_t status );

#ifdef __cplusplus
}

#include <raikv/util.h>
#include <raikv/work.h>
#include <raids/int_str.h>

namespace rai {
namespace ds {

typedef ds_msg_status_t RedisMsgStatus;
struct JsonInput;

struct RedisMsg : public ds_msg_s {

  RedisMsg &arr( size_t i ) const {
    return (RedisMsg &) this->array[ i ];
  }
  bool is_string( void ) const {
    return ( this->type == DS_SIMPLE_STRING || this->type == DS_BULK_STRING );
  }
  static inline bool valid_type_char( char b ) {
    switch ( b ) {
      case '$': case '*': case '+': case '-': case ':': return true;
      default: return false;
    }
  }
  static inline bool is_valid( ds_resp_type_t t ) {
    return valid_type_char( (char) t );
  }
  /* copy msg reference */
  void ref( RedisMsg &m ) {
    this->type = m.type;
    this->len  = m.len;
    if ( m.type == DS_INTEGER_VALUE )
      this->ival = m.ival;
    else if ( m.type == DS_BULK_ARRAY )
      this->array = m.array;
    else
      this->strval = m.strval;
  }
  RedisMsg *get_arg( int n ) const {
    const ds_msg_t *m = this;
    if ( m->type == DS_BULK_ARRAY ) {
      if ( n >= m->len )
        return NULL;
      m = &this->array[ n ];
    }
    return (RedisMsg *) m;
  }
  /* get the first string in an array: ["command"] */
  const char * command( size_t &length,  size_t &argc ) const {
    const char * str;
    argc = ( this->type == DS_BULK_ARRAY ) ? this->len : 1;
    if ( this->get_arg( 0, str, length ) )
      return str;
    return NULL;
  }
  const char * command( size_t &length ) const {
    size_t tmp;
    return this->command( length, tmp );
  }
  /* get a string argument and length */
  bool get_arg( int n,  const char *&str,  size_t &sz ) const {
    RedisMsg *m = this->get_arg( n );
    if ( m != NULL ) {
      if ( m->type == DS_BULK_STRING || m->type == DS_SIMPLE_STRING ) {
        if ( m->len > 0 ) {
          str = m->strval;
          sz  = m->len;
          return true;
        }
      }
    }
    str = NULL;
    sz  = 0;
    return false;
  }
  /* get an integer argument */
  bool get_arg( int n,  int64_t &i ) const {
    RedisMsg *m = this->get_arg( n );
    if ( m != NULL ) {
      if ( m->type == DS_BULK_STRING || m->type == DS_SIMPLE_STRING ) {
        if ( m->len > 0 )
          return str_to_int( m->strval, m->len, i ) == 0;
      }
      else if ( m->type == DS_INTEGER_VALUE ) {
        i = m->ival;
        return true;
      }
    }
    return false;
  }
  /* get an double argument */
  bool get_arg( int n,  double &f ) const {
    RedisMsg *m = this->get_arg( n );
    if ( m != NULL ) {
      if ( m->type == DS_BULK_STRING || m->type == DS_SIMPLE_STRING ) {
        if ( m->len > 0 )
          return str_to_dbl( m->strval, m->len, f ) == 0;
      }
      else if ( m->type == DS_INTEGER_VALUE ) {
        f = (double) m->ival;
        return true;
      }
    }
    return false;
  }
  /* match argument by string, returns which arg matched, 0 if none
   * the arg n indicates which arg
   *   int n = match( 1, "one", 3, "two", 3, NULL );
   *   if ( n == 1 ) matched one
   *   if ( n == 2 ) matched two
   *   if ( n == 0 ) matched none */
  #define MARG( str ) str, sizeof( str ) - 1
  /* used as: match_arg( 1, MARG( "hello" ), MARG( "world" ), NULL ) */
  size_t match_arg( size_t n,  const char *str,  size_t sz,
                    ... ) const noexcept;
  /* str length sz to int */
  static int str_to_int( const char *str,  size_t sz,  int64_t &ival ) {
    return rai::ds::string_to_int( str, sz, ival );
  }
  static int str_to_int( const char *str,  size_t sz,  int32_t &ival ) {
    int64_t j;
    int status;
    if ( (status = rai::ds::string_to_int( str, sz, j )) == 0 )
      ival = (int32_t) j;
    return status;
  }
  /* str length sz to uint */
  static int str_to_uint( const char *str,  size_t sz,  uint64_t &ival ) {
    return rai::ds::string_to_uint( str, sz, ival );
  }
  /* str length sz to double */
  static int str_to_dbl( const char *str,  size_t sz,  double &fval ) {
    return rai::ds::string_to_dbl( str, sz, fval );
  }
  /* various simple encodings used by redis */
  void set_nil( void ) {
    this->type   = DS_BULK_STRING;
    this->len    = -1;
    this->strval = NULL;
  }
  void set_null( void ) {
    this->type  = DS_BULK_ARRAY;
    this->len   = -1;
    this->array = NULL;
  }
  void set_mt_array( void ) {
    this->type  = DS_BULK_ARRAY;
    this->len   = 0;
    this->array = NULL;
  }
  void set_simple_string( char *s,  size_t sz = 0 ) {
    this->type   = DS_SIMPLE_STRING;
    this->len    = ( sz == 0 ? ::strlen( s ) : sz );
    this->strval = s;
  }
  void set_bulk_string( char *s,  size_t sz = 0 ) {
    this->type   = DS_BULK_STRING;
    this->len    = ( sz == 0 ? ::strlen( s ) : sz );
    this->strval = s;
  }
  void set_int( int64_t i ) {
    this->type = DS_INTEGER_VALUE;
    this->len  = 0;
    this->ival = i;
  }
  bool alloc_array( kv::ScratchMem &wrk,  int64_t sz ) noexcept;
  bool string_array( kv::ScratchMem &wrk,  int64_t sz,  ... ) noexcept;

  size_t pack_size( void ) const noexcept; /* pack() buf length */
  size_t pack( void *buf ) const noexcept;
  RedisMsgStatus pack2( void *buf,  size_t &len ) const noexcept; /* len is sz*/
  /* splits cmd line into array */
  RedisMsgStatus split( kv::ScratchMem &wrk ) noexcept;
  /* try to decode one message, length of data decoded is returned in len */
  RedisMsgStatus unpack( void *buf,  size_t &len,
                         kv::ScratchMem &wrk ) noexcept;
  /* same as above, but allow string based cmds without nl/cr */
  RedisMsgStatus unpack2( void *buf,  size_t len,
                          kv::ScratchMem &wrk ) noexcept;
  /* copy message into scratch mem */
  RedisMsg *dup( kv::ScratchMem &wrk ) noexcept;
  RedisMsg *dup2( kv::ScratchMem &wrk,  RedisMsg &cpy ) noexcept;
  /* similar to pack() and pack_size(), except in json format */
  size_t to_almost_json_size( bool be_weird = true ) const noexcept;
  size_t to_almost_json( char *buf,  bool be_weird = true ) const noexcept;

  /* decode json into msg */
  RedisMsgStatus unpack_json( const char *json,  kv::ScratchMem &wrk ) {
    size_t len = ::strlen( json );
    return this->unpack_json( json, len, wrk );
  }
  RedisMsgStatus unpack_json( const char *json,  size_t &len,
                              kv::ScratchMem &wrk ) noexcept;
  /* internal psuedo-json processing */
  RedisMsgStatus parse_json( JsonInput &input ) noexcept;
  RedisMsgStatus parse_object( JsonInput &input ) noexcept;
  RedisMsgStatus parse_array( JsonInput &input ) noexcept;
  RedisMsgStatus parse_string( JsonInput &input ) noexcept;
  RedisMsgStatus parse_number( JsonInput &input ) noexcept;
};

}
}
#endif /* __cplusplus */
#endif
