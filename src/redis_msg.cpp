#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <raids/redis_msg.h>

using namespace rai;
using namespace ds;
using namespace kv;

extern "C" {

size_t ds_uint_digits( uint64_t v ) { return uint64_digits( v ); }
size_t ds_int_digits( int64_t v ) { return int64_digits( v ); }
size_t ds_uint_to_string( uint64_t v,  char *buf,  size_t len ) {
  return uint64_to_string( v, buf, len );
}
size_t ds_int_to_string( int64_t v,  char *buf,  size_t len ) {
  return int64_to_string( v, buf, len );
}
int ds_string_to_int( const char *str,  size_t sz,  int64_t *ival ) {
  return string_to_int( str, sz, *ival );
}
int ds_string_to_uint( const char *str,  size_t sz,  uint64_t *ival ) {
  return string_to_uint( str, sz, *ival );
}
int ds_string_to_dbl( const char *str,  size_t sz,  double *fval ) {
  return string_to_dbl( str, sz, *fval );
}

const char *
ds_msg_status_string( ds_msg_status_t status ) {
  switch ( status ) {
    case DS_MSG_STATUS_OK:             return "OK";
    case DS_MSG_STATUS_BAD_TYPE:       return "BAD_TYPE";
    case DS_MSG_STATUS_PARTIAL:        return "PARTIAL";
    case DS_MSG_STATUS_ALLOC_FAIL:     return "ALLOC_FAIL";
    case DS_MSG_STATUS_BAD_JSON:       return "BAD_JSON";
    case DS_MSG_STATUS_BAD_INT:        return "BAD_INT";
    case DS_MSG_STATUS_INT_OVERFLOW:   return "INT_OVERFLOW";
    case DS_MSG_STATUS_BAD_FLOAT:      return "BAD_FLOAT";
    case DS_MSG_STATUS_FLOAT_OVERFLOW: return "FLOAT_OVERFLOW";
  }
  return "UNKNOWN";
}

const char *
ds_msg_status_description( ds_msg_status_t status ) {
  switch ( status ) {
    case DS_MSG_STATUS_OK:         return "OK";
    case DS_MSG_STATUS_BAD_TYPE: return "Message decoding error, bad type char";
    case DS_MSG_STATUS_PARTIAL:    return "Partial value";
    case DS_MSG_STATUS_ALLOC_FAIL: return "Alloc failed";
    case DS_MSG_STATUS_BAD_JSON:   return "Unable to parse JSON message";
    case DS_MSG_STATUS_BAD_INT:    return "Unable to parse integer";
    case DS_MSG_STATUS_INT_OVERFLOW: return "Integer overflow";
    case DS_MSG_STATUS_BAD_FLOAT:  return "Unable to parse float number";
    case DS_MSG_STATUS_FLOAT_OVERFLOW: return "Float number overflow";
  }
  return "Unknown msg status";
}

} /* extern "C" */

RedisMsgStatus
RedisMsg::pack2( void *buf,  size_t &buflen ) const noexcept
{
  char  * ptr = (char *) buf;
  size_t  i;

  if ( this->type == DS_BULK_STRING || this->type == DS_BULK_ARRAY ) {
    i = int64_digits( this->len );
    if ( i + 2 + ( this->len > 0 ? this->len : 0 ) + 2 >= buflen )
      return DS_MSG_STATUS_PARTIAL;
    ptr[ 0 ] = (char) this->type;
    i = 1 + int64_to_string( this->len, &ptr[ 1 ], i );

    if ( this->type == DS_BULK_STRING ) {
      if ( this->len >= 0 ) {
        i = crlf( ptr, i );
        ::memcpy( &ptr[ i ], this->strval, this->len );
        i += this->len;
      }
    }
    else {
      i = crlf( ptr, i );
      if ( this->len >= 0 ) {
        for ( size_t k = 0; k < (size_t) this->len; k++ ) {
          size_t tmp = buflen - i;
          RedisMsgStatus stat = this->arr( k ).pack2( &ptr[ i ], tmp );
          if ( stat != DS_MSG_STATUS_OK )
            return stat;
          i += tmp;
        }
      }
      buflen = i;
      return DS_MSG_STATUS_OK; /* no trailing crlf */
    }
  }
  else if ( this->type == DS_INTEGER_VALUE ) {
    i = int64_digits( this->ival );
    if ( i + 2 >= buflen )
      return DS_MSG_STATUS_PARTIAL;
    ptr[ 0 ] = (char) DS_INTEGER_VALUE;
    i = 1 + int64_to_string( this->ival, &ptr[ 1 ], i );
  }
  else if ( this->type == DS_SIMPLE_STRING ||
            this->type == DS_ERROR_STRING ) {
    i = 1 + this->len;
    if ( i + 2 >= buflen )
      return DS_MSG_STATUS_PARTIAL;
    ptr[ 0 ] = (char) this->type;
    ::memcpy( &ptr[ 1 ], this->strval, this->len );
  }
  else {
    return DS_MSG_STATUS_BAD_TYPE;
  }
  buflen = crlf( ptr, i );
  return DS_MSG_STATUS_OK;
}

size_t
RedisMsg::pack( void *buf ) const noexcept
{
  char * ptr = (char *) buf;
  size_t i;

  if ( this->type == DS_BULK_STRING || this->type == DS_BULK_ARRAY ) {
    ptr[ 0 ] = (char) this->type;
    i = 1 + int64_to_string( this->len, &ptr[ 1 ] );
    if ( this->type == DS_BULK_STRING ) {
      if ( this->len >= 0 ) {
        i = crlf( ptr, i );
        ::memcpy( &ptr[ i ], this->strval, this->len );
        i += this->len;
      }
    }
    else {
      i = crlf( ptr, i );
      if ( this->len >= 0 ) {
        for ( size_t k = 0; k < (size_t) this->len; k++ )
          i += this->arr( k ).pack( &ptr[ i ] );
      }
      return i; /* no trailing crlf */
    }
  }
  else if ( this->type == DS_INTEGER_VALUE ) {
    ptr[ 0 ] = (char) DS_INTEGER_VALUE;
    i = 1 + int64_to_string( this->ival, &ptr[ 1 ] );
  }
  else if ( this->type == DS_SIMPLE_STRING ||
            this->type == DS_ERROR_STRING ) {
    ptr[ 0 ] = (char) this->type;
    i = 1 + this->len;
    ::memcpy( &ptr[ 1 ], this->strval, this->len );
  }
  else {
    return DS_MSG_STATUS_BAD_TYPE;
  }
  return crlf( ptr, i );
}

size_t
RedisMsg::pack_size( void ) const noexcept
{
  size_t i;

  if ( this->type == DS_BULK_STRING || this->type == DS_BULK_ARRAY ) {
    i = 1 + int64_digits( this->len );
    if ( this->type == DS_BULK_STRING ) {
      if ( this->len >= 0 )
        i += 2 + this->len;
      return i + 2;
    }
    i += 2;
    if ( this->len >= 0 ) {
      for ( size_t k = 0; k < (size_t) this->len; k++ )
        i += this->arr( k ).pack_size();
    }
    return i;
  }
  if ( this->type == DS_INTEGER_VALUE )
    return 1 + int64_digits( this->ival ) + 2;

  if ( this->type == DS_SIMPLE_STRING || this->type == DS_ERROR_STRING )
    return 1 + this->len + 2;
  return 0;
}

bool
RedisMsg::alloc_array( ScratchMem &wrk,  int64_t sz ) noexcept
{
  this->type  = DS_BULK_ARRAY;
  this->array = NULL;
  if ( (this->len = sz) < 0 )
    this->len = -1;
  else if ( sz > 0 ) {
    this->array = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * sz );
    if ( this->array == NULL )
      return false;
  }
  return true;
}

bool
RedisMsg::string_array( ScratchMem &wrk,  int64_t sz,  ... ) noexcept
{
  if ( ! this->alloc_array( wrk, sz ) )
    return false;
  if ( sz > 0 ) {
    va_list args;
    int64_t k = 0;
    va_start( args, sz );
    do {
      this->array[ k ].type   = DS_BULK_STRING;
      this->array[ k ].len    = va_arg( args, size_t );
      this->array[ k ].strval = va_arg( args, char * );
    } while ( ++k < sz );
    va_end( args );
  }
  return true;
}
/* Split a string into argv[]/argc, used when message is not structured */
RedisMsgStatus
RedisMsg::split( ScratchMem &wrk ) noexcept
{
  RedisMsg * tmp = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * 4 );
  char     * ptr = this->strval,
           * end = &ptr[ this->len ];
  size_t     cnt = 0;

  if ( tmp == NULL )
    return DS_MSG_STATUS_ALLOC_FAIL;
  for ( ; ; ptr++ ) {
    if ( ptr >= end )
      goto finished;
    if ( *ptr > ' ' )
      break;
  }
  tmp[ 0 ].strval = ptr;
  for (;;) {
    if ( ++ptr == end || *ptr <= ' ' ) {
      tmp[ cnt ].type = DS_BULK_STRING;
      tmp[ cnt ].len  = ptr - tmp[ cnt ].strval;
      cnt++;
      while ( ptr < end && *ptr <= ' ' )
        ptr++; 
      if ( ptr == end )
        goto finished;
      if ( cnt % 4 == 0 ) {
        RedisMsg * tmp2 = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * ( cnt + 4 ) );
        if ( tmp2 == NULL )
          return DS_MSG_STATUS_ALLOC_FAIL;
        ::memcpy( tmp2, tmp, sizeof( RedisMsg ) * cnt );
        tmp = tmp2;
      }
      tmp[ cnt ].strval = ptr;
    } 
  }   
finished:;
  this->type  = DS_BULK_ARRAY;
  this->len   = cnt;
  this->array = tmp;
  return DS_MSG_STATUS_OK;
}
/* Split buffer bytes into a message */
RedisMsgStatus
RedisMsg::unpack( void *buf,  size_t &buflen,  ScratchMem &wrk ) noexcept
{
  char  * ptr, * eol;
  size_t  i, j, off = 0, bsz = buflen;
  RedisMsgStatus status;

  /* skip over whitespace */
  ptr = (char *) buf;
  for ( off = 0; off < bsz; off++ )
    if ( (uint8_t) ptr[ off ] > ' ' )
       break;

  /* find a newline */
  ptr  = &ptr[ off ];
  bsz -= off;
  for ( eol = &ptr[ 1 ]; ; eol++ ) {
    if ( eol >= &ptr[ bsz ] )
      return DS_MSG_STATUS_PARTIAL;
    if ( *eol == '\n' )
      break;
  }

  /* i = count of chars without \r\n, j = count of chars to eol */
  i = eol - &ptr[ 1 ];
  j = i + 2;
  if ( ptr[ i ] == '\r' )
    i--;
  this->type = (ds_resp_type_t) ptr[ 0 ];
  if ( this->type == DS_BULK_STRING || this->type == DS_BULK_ARRAY ) {
    /* a bulk string has lengths */
    status = (RedisMsgStatus) str_to_int( &ptr[ 1 ], i, this->len );
    if ( status != DS_MSG_STATUS_OK )
      return status;

    if ( this->type == DS_BULK_STRING ) {
      /* test length */
      if ( this->len > 0 ) {
        this->strval = &ptr[ j ];
        j += this->len;
        if ( j > bsz )
          return DS_MSG_STATUS_PARTIAL;
      }
      /* if len <= 0, zero length */
      else
        this->strval = NULL;
      /* allow bulk without seeing the nl */
      if ( this->len >= 0 ) {
        if ( j < bsz && ptr[ j ] == '\r' )
          j++;
        if ( j < bsz && ptr[ j ] == '\n' )
          j++;
      }
    }
    else {
      /* allocate and recursively parse the elements */
      if ( this->len > 0 ) {
        this->array = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * this->len );
        if ( this->array == NULL )
          return DS_MSG_STATUS_ALLOC_FAIL;
        for ( size_t k = 0; k < (size_t) this->len; k++ ) {
          if ( bsz <= j )
            return DS_MSG_STATUS_PARTIAL;
          size_t tmp = bsz - j;
          RedisMsgStatus stat;
          stat = this->arr( k ).unpack( &ptr[ j ], tmp, wrk );
          if ( stat != DS_MSG_STATUS_OK )
            return stat;
          j += tmp; /* accumulate the message size */
        }
      }
      /* if len <= 0, zero length */
      else
        this->array = NULL;
    }
  }
  else if ( this->type == DS_INTEGER_VALUE ) {
    /* an int64 */
    this->len = 0; /* could set this to i */
    status = (RedisMsgStatus) str_to_int( &ptr[ 1 ], i, this->ival );
    if ( status != DS_MSG_STATUS_OK )
      return status;
  }
  else if ( this->type == DS_SIMPLE_STRING ||
            this->type == DS_ERROR_STRING ) {
    /* no lengths, simple string or error */
    this->len = i;
    this->strval = &ptr[ 1 ];
  }
  else {
    /* inline command */
    this->type   = DS_SIMPLE_STRING;
    this->len    = i + 1;
    this->strval = ptr;
    buflen = j + off;
    return this->split( wrk );
  }
  buflen = j + off;
  return DS_MSG_STATUS_OK;
}
/* Split buffer bytes into a message */
RedisMsgStatus
RedisMsg::unpack2( void *buf,  size_t bsz,  ScratchMem &wrk ) noexcept
{
  char  * ptr, * eol;
  size_t  i, j, off = 0;
  RedisMsgStatus status;

  /* skip over whitespace */
  ptr = (char *) buf;
  for ( off = 0; off < bsz; off++ )
    if ( (uint8_t) ptr[ off ] > ' ' )
       break;

  /* find a newline */
  ptr  = &ptr[ off ];
  bsz -= off;
  for ( eol = ptr; ; eol++ ) {
    if ( eol == &ptr[ bsz ] ) {
      i = bsz;
      j = bsz;
      break;
    }
    if ( *eol == '\n' ) {
      /* i = count of chars without \r\n, j = count of chars to eol */
      i = eol - &ptr[ 1 ];
      j = i + 1;
      if ( ptr[ i ] == '\r' ) {
        i--;
        j++;
      }
      break;
    }
  }

  this->type = (ds_resp_type_t) ptr[ 0 ];
  if ( this->type == DS_BULK_STRING || this->type == DS_BULK_ARRAY ) {
    /* a bulk string has lengths */
    status = (RedisMsgStatus) str_to_int( &ptr[ 1 ], i, this->len );
    if ( status != DS_MSG_STATUS_OK )
      return status;

    if ( this->type == DS_BULK_STRING ) {
      /* test length */
      if ( this->len > 0 ) {
        this->strval = &ptr[ j ];
        j += this->len;
        if ( j > bsz )
          return DS_MSG_STATUS_PARTIAL;
      }
      /* if len <= 0, zero length */
      else
        this->strval = NULL;
    }
    else {
      /* allocate and recursively parse the elements */
      if ( this->len > 0 ) {
        this->array = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * this->len );
        if ( this->array == NULL )
          return DS_MSG_STATUS_ALLOC_FAIL;
        for ( size_t k = 0; k < (size_t) this->len; k++ ) {
          if ( bsz <= j )
            return DS_MSG_STATUS_PARTIAL;
          size_t tmp = bsz - j;
          RedisMsgStatus stat;
          stat = this->arr( k ).unpack( &ptr[ j ], tmp, wrk );
          if ( stat != DS_MSG_STATUS_OK )
            return stat;
          j += tmp; /* accumulate the message size */
        }
      }
      /* if len <= 0, zero length */
      else
        this->array = NULL;
    }
  }
  else if ( this->type == DS_INTEGER_VALUE ) {
    /* an int64 */
    this->len = 0; /* could set this to i */
    status = (RedisMsgStatus) str_to_int( &ptr[ 1 ], i, this->ival );
    if ( status != DS_MSG_STATUS_OK )
      return status;
  }
  else if ( this->type == DS_SIMPLE_STRING ||
            this->type == DS_ERROR_STRING ) {
    /* no lengths, simple string or error */
    this->len = i;
    this->strval = &ptr[ 1 ];
  }
  else {
    /* inline command */
    this->type   = DS_SIMPLE_STRING;
    this->len    = i + 1;
    this->strval = ptr;
    return this->split( wrk );
  }
  return DS_MSG_STATUS_OK;
}

RedisMsg *
RedisMsg::dup( ScratchMem &wrk ) noexcept
{
  RedisMsg *cpy = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) );
  if ( cpy == NULL )
    return NULL;
  return this->dup2( wrk, *cpy );
}

RedisMsg *
RedisMsg::dup2( ScratchMem &wrk,  RedisMsg &cpy ) noexcept
{
  /* recurse and copy the message into work mem */
  cpy.type = this->type;
  cpy.len  = this->len;
  if ( this->type == DS_INTEGER_VALUE ) {
    cpy.ival = this->ival;
  }
  else if ( this->type != DS_BULK_ARRAY ) {
    if ( this->len < 0 )
      cpy.strval = NULL;
    else {
      cpy.strval = (char *) wrk.alloc( this->len + 1 );
      if ( cpy.strval == NULL )
        return NULL;
      ::memcpy( cpy.strval, this->strval, this->len );
      cpy.strval[ this->len ] = '\0';
    }
  }
  else {
    if ( this->len <= 0 )
      cpy.array = NULL;
    else {
      cpy.array = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * this->len );
      if ( cpy.array == NULL )
        return NULL;
      for ( size_t i = 0; i < (size_t) this->len; i++ )
        if ( this->arr( i ).dup2( wrk, cpy.arr( i ) ) == NULL )
          return NULL;
    }
  }
  return &cpy;
}

size_t
RedisMsg::match_arg( size_t n,  const char *str,  size_t sz,
                     ... ) const noexcept
{
  /* match strings at arg position n >= 0 && n < len */
  if ( this->len <= 0 || n >= (size_t) this->len ) /* len could be negative */
    return 0;

  size_t k;
  va_list args;
  const RedisMsg & m = this->arr( n );

  if ( ! m.is_string() )
    return 0;

  va_start( args, sz );
  for ( k = 1; ; k++ ) {
    if ( (size_t) m.len == sz && ::strncasecmp( str, m.strval, sz ) == 0 )
      break; /* match */
    str = va_arg( args, const char * );
    /* args are terminated with NULL */
    if ( str == NULL ) {
      k = 0; /* no match */
      break;
    }
    sz = va_arg( args, size_t );
  }
  va_end( args );
  return k;
}

int
rai::ds::string_to_int( const char *str,  size_t sz,  int64_t &ival ) noexcept
{
  /* max is 1844674407,3709551615, this table doesnn't overflow 32bits */
  static const uint32_t pow10[] = {     10000U * 10000U * 10,
    10000U * 10000, 10000U * 1000, 10000U * 100, 10000U * 10,
             10000,          1000,          100,          10,
                 1
  };
  static const size_t max_pow10 = sizeof( pow10 ) / sizeof( pow10[ 0 ] );
  size_t i, j;
  uint64_t n = 0;
  bool neg;

  if ( sz == 0 )
    return DS_MSG_STATUS_BAD_INT;
  if ( str[ 0 ] == '-' ) {
    if ( --sz == 0 )
      return DS_MSG_STATUS_BAD_INT;
    str++;
    neg = true;
  }
  else {
    neg = false;
  }
  i = j = 0;
  if ( sz < max_pow10 )
    i = max_pow10 - sz;
  else
    j = sz - max_pow10;
  sz = j;
  for (;;) {
    if ( str[ j ] < '0' || str[ j ] > '9' )
      return DS_MSG_STATUS_BAD_INT;
    n += (uint64_t) pow10[ i++ ] * ( str[ j++ ] - '0' );
    if ( i == max_pow10 )
      break;
  }
  if ( sz != 0 ) {
    i = j = 0;
    if ( sz <= max_pow10 )
      i = max_pow10 - sz;
    else
      return DS_MSG_STATUS_INT_OVERFLOW;
    uint64_t nn = 0;
    for (;;) {
      if ( str[ j ] < '0' || str[ j ] > '9' )
        return DS_MSG_STATUS_BAD_INT;
      nn += (uint64_t) pow10[ i++ ] * ( str[ j++ ] - '0' );
      if ( i == max_pow10 )
        break;
    }
    /* 922337203,6854775807 == 0x7fffffffffffffff */
    if ( nn > (uint64_t) 922337203 ||
         ( nn == (uint64_t) 922337203 &&
           n > (uint64_t) 6854775807 ) ) {
      /* test for neg && 6854775808 */
      if ( neg && nn == 922337203 && n == 6854775808 ) {
        ival = 0x8000000000000000LL;
        return DS_MSG_STATUS_OK;
      }
      return DS_MSG_STATUS_INT_OVERFLOW;
    }
    n += (uint64_t) 10 * (uint64_t) pow10[ 0 ] * nn;
  }
  if ( neg )
    ival = -(int64_t) n;
  else
    ival = (int64_t) n;
  return DS_MSG_STATUS_OK;
}

int
rai::ds::string_to_dbl( const char *str,  size_t sz,  double &fval ) noexcept
{
  char buf[ 64 ], *endptr = NULL;
  /* null terminate string */
  if ( sz > sizeof( buf ) - 1 )
    sz = sizeof( buf ) - 1;
  ::memcpy( buf, str, sz );
  buf[ sz ] = '\0';
  fval = ::strtod( buf, &endptr );
  if ( endptr == buf )
    return DS_MSG_STATUS_BAD_FLOAT;
  if ( fval == 0 && errno == ERANGE )
    return DS_MSG_STATUS_FLOAT_OVERFLOW;
  return DS_MSG_STATUS_OK;
}

int
rai::ds::string_to_uint( const char *str,  size_t sz,  uint64_t &ival ) noexcept
{
  /* max is 1844674407,3709551615, this table doesnn't overflow 32bits */
  static const uint32_t pow10[] = {     10000U * 10000U * 10,
    10000U * 10000, 10000U * 1000, 10000U * 100, 10000U * 10,
             10000,          1000,          100,          10,
                 1
  };
  static const size_t max_pow10 = sizeof( pow10 ) / sizeof( pow10[ 0 ] );
  size_t i, j;
  uint64_t n = 0;
  bool neg;

  if ( sz == 0 )
    return DS_MSG_STATUS_BAD_INT;
  if ( str[ 0 ] == '-' ) {
    if ( --sz == 0 )
      return DS_MSG_STATUS_BAD_INT;
    str++;
    neg = true;
  }
  else {
    neg = false;
  }
  i = j = 0;
  if ( sz < max_pow10 )
    i = max_pow10 - sz;
  else
    j = sz - max_pow10;
  sz = j;
  for (;;) {
    if ( str[ j ] < '0' || str[ j ] > '9' )
      return DS_MSG_STATUS_BAD_INT;
    n += (uint64_t) pow10[ i++ ] * ( str[ j++ ] - '0' );
    if ( i == max_pow10 )
      break;
  }
  if ( sz != 0 ) {
    i = j = 0;
    if ( sz <= max_pow10 )
      i = max_pow10 - sz;
    else
      return DS_MSG_STATUS_INT_OVERFLOW;
    uint64_t nn = 0;
    for (;;) {
      if ( str[ j ] < '0' || str[ j ] > '9' )
        return DS_MSG_STATUS_BAD_INT;
      nn += (uint64_t) pow10[ i++ ] * ( str[ j++ ] - '0' );
      if ( i == max_pow10 )
        break;
    }
    if ( neg ) {
      /* 922337203,6854775807 == 0x7fffffffffffffff */
      if ( nn > (uint64_t) 922337203 ||
           ( nn == (uint64_t) 922337203 &&
             n > (uint64_t) 6854775807 ) ) {
        /* test for neg && 6854775808 */
        if ( neg && nn == 922337203 && n == 6854775808 ) {
          ival = 0x8000000000000000LL;
          return DS_MSG_STATUS_OK;
        }
        return DS_MSG_STATUS_INT_OVERFLOW;
      }
    }
    else {
      /* 1844674407,3709551615 == 0xffffffffffffffff */
      if ( nn > (uint64_t) 1844674407 ||
           ( nn == (uint64_t) 1844674407 &&
             n > (uint64_t) 3709551615 ) ) {
        return DS_MSG_STATUS_INT_OVERFLOW;
      }
    }
    n += (uint64_t) 10 * (uint64_t) pow10[ 0 ] * nn;
  }
  if ( neg )
    ival = (uint64_t) -(int64_t) n;
  else
    ival = n;
  return DS_MSG_STATUS_OK;
}

static size_t
json_escape_strlen( const char *str,  size_t len )
{
  size_t sz = 0;
  for ( size_t i = 0; i < len; i++ ) {
    if ( (uint8_t) str[ i ] >= ' ' && (uint8_t) str[ i ] <= 126 ) {
      switch ( str[ i ] ) {
        case '\'':
        case '"': sz++; /* FALLTHRU */
        default:  sz++; break;
      }
    }
    else {
      switch ( str[ i ] ) {
        case '\b':
        case '\f': 
        case '\n': 
        case '\r': 
        case '\t': sz += 2; break;
        default:   sz += 6; break;
      }
    }
  }
  return sz;
}

static size_t
json_escape_string( const char *str,  size_t len,  char *out )
{
  size_t sz = 0;
  for ( size_t i = 0; i < len; i++ ) {
    if ( (uint8_t) str[ i ] >= ' ' && (uint8_t) str[ i ] <= 126 ) {
      switch ( str[ i ] ) {
        case '\'':
        case '"': out[ sz++ ] = '\\'; /* FALLTHRU */
        default:  out[ sz++ ] = str[ i ]; break;
      }
    }
    else {
      out[ sz ] = '\\';
      switch ( str[ i ] ) {
        case '\b': out[ sz + 1 ] = 'b'; sz += 2; break;
        case '\f': out[ sz + 1 ] = 'f'; sz += 2; break;
        case '\n': out[ sz + 1 ] = 'n'; sz += 2; break;
        case '\r': out[ sz + 1 ] = 'r'; sz += 2; break;
        case '\t': out[ sz + 1 ] = 't'; sz += 2; break;
        default:   out[ sz + 1 ] = 'u';
                   out[ sz + 2 ] = '0';
                   out[ sz + 3 ] = '0' + ( ( (uint8_t) str[ i ] / 100 ) % 10 );
                   out[ sz + 4 ] = '0' + ( ( (uint8_t) str[ i ] / 10 ) % 10 );
                   out[ sz + 5 ] = '0' + ( (uint8_t) str[ i ] % 10 );
                   sz += 6; break;
      }
    }
  }
  return sz;
}

/* uses different quote styles for various strings (if be_weird = true):
 *   " for simple, ` for error, ' for bulk strings
 * null = -1 sized array
 * nil  = -1 sized string
 *
 * does not null terminate strings;
 * use to_almost_json_size() to determine the necessary length of the buffer
 */
size_t
RedisMsg::to_almost_json( char *buf,  bool be_weird ) const noexcept
{
  size_t elen;
  char   q;

  switch ( this->type ) {
    case DS_SIMPLE_STRING: q = '\''; elen = 1; if ( 0 ) {
    case DS_ERROR_STRING:  q = '`';  elen = 1; if ( 0 ) {
    case DS_BULK_STRING:   q = '"';  elen = 0; } }
      if ( this->len >= 0 ) {
        if ( ! be_weird ) { /* normal quoting */
          buf[ 0 ] = '"';
          if ( elen != 0 )
            buf[ 1 ] = (char) this->type; /* simple and error have marker */
        }
        else {
          buf[ 0 ] = q; /* weird quoting */
          elen = 0;
        }
        elen += json_escape_string( this->strval, this->len, &buf[ elen + 1 ] );
        buf[ 1 + elen ] = ( be_weird ? q : '"' );
        buf[ 2 + elen ] = '\0';
        return elen + 2;
      }
      if ( be_weird ) {
        ::memcpy( buf, "nil", 3 ); /* weird null */
        return 3;
      }
      ::memcpy( buf, "null", 4 );
      return 4;

    case DS_INTEGER_VALUE:
      return int64_to_string( this->ival, buf );

    case DS_BULK_ARRAY:
      if ( this->len >= 0 ) {
        elen = 1;
        buf[ 0 ] = '[';
        if ( this->len > 0 )
          elen += this->arr( 0 ).to_almost_json( &buf[ elen ], be_weird );
        for ( size_t i = 1; i < (size_t) this->len; i++ ) {
          buf[ elen++ ] = ',';
          elen += this->arr( i ).to_almost_json( &buf[ elen ], be_weird );
        }
        buf[ elen ] = ']';
        return elen + 1;
      }
      ::memcpy( buf, "null", 4 );
      return 4;

    default:
      return 0;
  }
}

size_t
RedisMsg::to_almost_json_size( bool be_weird ) const noexcept
{
  size_t elen;

  switch ( this->type ) {
    case DS_SIMPLE_STRING: elen = 1; if ( 0 ) {
    case DS_ERROR_STRING:  elen = 1; if ( 0 ) {
    case DS_BULK_STRING:   elen = 0; } }
      if ( this->len >= 0 ) {
        if ( be_weird ) /* normal quoting */
          elen = 0;
        elen += json_escape_strlen( this->strval, this->len );
        return elen + 2;
      }
      if ( be_weird ) /* weird null (nil) */
        return 3;
      return 4; /* null */

    case DS_INTEGER_VALUE:
      return int64_digits( this->ival );

    case DS_BULK_ARRAY:
      if ( this->len >= 0 ) {
        size_t sz;
        elen = 1;
        if ( this->len > 0 ) {
          sz = this->arr( 0 ).to_almost_json_size( be_weird );
          if ( sz == 0 )
            return 0;
          elen += sz;
        }
        for ( size_t i = 1; i < (size_t) this->len; i++ ) {
          sz = this->arr( i ).to_almost_json_size( be_weird );
          if ( sz == 0 )
            return 0;
          elen += 1 + sz;
        }
        return elen + 1;
      }
      return 4; /* null */
    default:
      return 0;
  }
}

namespace rai {
namespace ds {
static const int JSON_EOF = 256;
struct JsonInput {
  const char * json;
  size_t       offset,
               length;
  uint32_t     lineStart,
               lineCount;
  ScratchMem & wrk;

  int cur( void ) {
    return ( this->offset < this->length ) ?
      (int) (uint8_t) this->json[ this->offset ] : JSON_EOF;
  }
  int next( void ) {
    return ( this->offset < this->length ) ?
      (int) (uint8_t) this->json[ this->offset++ ] : JSON_EOF;
  }
  int forward( void ) {
    return ( ++this->offset < this->length ) ?
      (int) (uint8_t) this->json[ this->offset ] : JSON_EOF;
  }
  void consume( size_t len ) { this->offset += len; }
  bool match( char c1,  char c2,  char c3,  char c4,  char c5 ) noexcept;
  int  eat_white( void ) noexcept;

  JsonInput( ScratchMem &w,  const char *js = NULL,  size_t off = 0,
             size_t len = 0 ) : wrk( w ) {
    this->init( js, off, len );
  }
  void init( const char *js,  size_t off,  size_t len ) {
    this->json      = js;
    this->offset    = off;
    this->length    = len;
    this->lineStart = 0;
    this->lineCount = 0;
  }
  void * alloc( size_t sz ) {
    return this->wrk.alloc( sz );
  }
  void * extend( void *obj,  size_t oldsz,  size_t addsz ) {
    void * p = this->wrk.alloc( oldsz + addsz );
    if ( p != NULL )
      ::memcpy( p, obj, oldsz );
    return p;
  }
};
}
}

RedisMsgStatus
RedisMsg::unpack_json( const char *json,  size_t &len,
                       ScratchMem &wrk ) noexcept
{
  JsonInput input( wrk, json, 0, len );
  RedisMsgStatus status = this->parse_json( input );
  if ( status == DS_MSG_STATUS_OK ) {
    len = input.offset;
    return DS_MSG_STATUS_OK;
  }
  return status;
}

int
JsonInput::eat_white( void ) noexcept
{
  int c = this->cur();
  if ( isspace( c ) ) {
    do {
      if ( c == '\n' ) {
        this->lineCount++;
        this->lineStart = this->offset + 1;
      }
      c = this->forward();
    } while ( isspace( c ) );
  }
  return c;
}

bool
JsonInput::match( char c1,  char c2,  char c3,  char c4,  char c5 ) noexcept
{
  if ( this->offset + 3 > this->length ||
       c1 != this->json[ this->offset ] ||
       c2 != this->json[ this->offset + 1 ] ||
       c3 != this->json[ this->offset + 2 ] )
    return false;
  if ( c4 == 0 )
    return true;
  if ( this->offset + 4 > this->length ||
       c4 != this->json[ this->offset + 3 ] )
    return false;
  return ( c5 == 0 ||
           ( this->offset + 5 <= this->length &&
             c5 == this->json[ this->offset + 4 ] ) );
}

RedisMsgStatus
RedisMsg::parse_json( JsonInput &input ) noexcept
{
  int c = input.eat_white();
  switch ( c ) {
    case '{': return this->parse_object( input );
    case '[': return this->parse_array( input );
    case '\'': case '`':
    case '"': return this->parse_string( input );
    case '0': case '1': case '2': case '3': case '4': case '5':
    case '6': case '7': case '8': case '9': case '-':
      return this->parse_number( input );
    case JSON_EOF: return DS_MSG_STATUS_PARTIAL;
    case 't': if ( input.match( 't', 'r', 'u', 'e', 0 ) ) {
                this->type = DS_INTEGER_VALUE;
                this->len  = 0;
                this->ival = 1;
                input.consume( 4 );
                return DS_MSG_STATUS_OK;
              if ( 0 ) {
    case 'f': if ( input.match( 'f', 'a', 'l', 's', 'e' ) ) {
                this->type = DS_INTEGER_VALUE;
                this->len  = 0;
                this->ival = 0;
                input.consume( 5 );
                return DS_MSG_STATUS_OK;
              if ( 0 ) {
    case 'n': if ( input.match( 'n', 'u', 'l', 'l', 0 ) ) {
                this->type  = DS_BULK_ARRAY;
                this->len   = -1;
                this->array = NULL;
                input.consume( 4 );
                return DS_MSG_STATUS_OK;
              }
              else if ( input.match( 'n', 'i', 'l', 0, 0 ) ) {
                this->type   = DS_BULK_STRING;
                this->len    = -1;
                this->strval = NULL;
                input.consume( 3 );
                return DS_MSG_STATUS_OK;
              }
              } } } }
              /* FALLTHRU */
    default:
      return DS_MSG_STATUS_BAD_JSON;
  }
}

RedisMsgStatus
RedisMsg::parse_object( JsonInput & ) noexcept
{
  /* no way of representing objects */
  return DS_MSG_STATUS_BAD_JSON;
}

RedisMsgStatus
RedisMsg::parse_array( JsonInput &input ) noexcept
{
  RedisMsgStatus status;
  size_t   sz  = 0;
  uint32_t tos = 0,
           i   = 0,
           j;
  RedisMsg value,
         * val[ 40 ],
         * end[ 40 ];

  val[ 0 ] = NULL;
  end[ 0 ] = NULL;
  input.next(); /* eat '[' */
  int c = input.eat_white();
  while ( c != ']' ) {
    if ( (status = value.parse_json( input )) != DS_MSG_STATUS_OK )
      return status;

    if ( tos == 0 || &val[ tos ][ i ] == end[ tos ] ) {
      size_t newsz = ( sz + 2 ) * 3 / 2;
      tos++;
      if ( tos == sizeof( val ) / sizeof( val[ 0 ] ) )
        return DS_MSG_STATUS_BAD_JSON;
      val[ tos ] = (RedisMsg *) input.alloc( sizeof( RedisMsg ) * newsz );
      if ( val[ tos ] == NULL )
        return DS_MSG_STATUS_ALLOC_FAIL;
      end[ tos ] = &val[ tos ][ newsz ];
      sz = newsz;
      i  = 0;
    }
    val[ tos ][ i ] = value;
    i++;

    c = input.eat_white();
    if ( c != ',' )
      break;
    input.next(); /* eat ',' */
    c = input.eat_white();
  }
  if ( c != ']' ) {
    if ( c == JSON_EOF )
      return DS_MSG_STATUS_PARTIAL;
    return DS_MSG_STATUS_BAD_JSON;
  }
  input.next(); /* eat ']' */

  if ( tos > 0 ) {
    if ( tos == 1 ) {
      this->type  = DS_BULK_ARRAY;
      this->len   = i;
      this->array = val[ 1 ];
    }
    else {
      sz = i;
      for ( j = 1; j < tos; j++ )
        sz += end[ j ] - val[ j ];
      this->type  = DS_BULK_ARRAY;
      this->len   = sz;
      this->array = (RedisMsg *) input.alloc( sizeof( RedisMsg ) * sz );
      if ( this->array == NULL )
        return DS_MSG_STATUS_ALLOC_FAIL;
      sz = 0;
      for ( j = 1; j < tos; j++ ) {
        ::memcpy( &this->array[ sz ], val[ j ],
                  ( end[ j ] - val[ j ] ) * sizeof( RedisMsg ) );
        sz += end[ j ] - val[ j ];
      }
      ::memcpy( &this->array[ sz ], val[ tos ], i * sizeof( RedisMsg ) );
    }
  }
  else {
    this->type  = DS_BULK_ARRAY;
    this->len   = 0;
    this->array = NULL;
  }
  return DS_MSG_STATUS_OK;
}

static inline uint32_t
hex_value( int c )
{
  if ( c >= '0' && c <= '9' )
    return (uint32_t) ( c - '0' );
  if ( c >= 'a' && c <= 'f' )
    return (uint32_t) ( c - 'a' + 10 );
  if ( c >= 'A' && c <= 'F' )
    return (uint32_t) ( c - 'A' + 10 );
  return 0xffU;
}

RedisMsgStatus
RedisMsg::parse_string( JsonInput &input ) noexcept
{
  size_t sz = 8;
  char * str,
       * end;
  int    quote;

  str = this->strval = (char *) input.alloc( 8 );
  if ( str == NULL )
    return DS_MSG_STATUS_ALLOC_FAIL;
  end = &str[ 8 ];
  quote = input.next(); /* eat '"' */
  for (;;) {
    int c = input.next();
    if ( c == JSON_EOF )
      return DS_MSG_STATUS_PARTIAL;
    if ( str == end ) {
      this->strval = (char *) input.extend( this->strval, sz, 16 );
      if ( this->strval == NULL )
        return DS_MSG_STATUS_ALLOC_FAIL;
      str = &this->strval[ sz ];
      sz += 16;
      end = &str[ 16 ];
    }
    if ( c == quote ) {
      *str = '\0';
      if ( quote == '"' )
        this->type = DS_BULK_STRING;
      else if ( quote == '\'' )
        this->type = DS_SIMPLE_STRING;
      else
        this->type = DS_ERROR_STRING;
      this->len  = (int64_t) ( str - this->strval );
      return DS_MSG_STATUS_OK;
    }
    if ( c != '\\' ) {
      *str++ = (char) c;
      continue;
    }

    int b = input.next(); /* escaped char */
    switch ( b ) {
      case 'b': *str++ = '\b'; break;
      case 'f': *str++ = '\f'; break;
      case 'n': *str++ = '\n'; break;
      case 'r': *str++ = '\r'; break;
      case 't': *str++ = '\t'; break;
      default:  *str++ = (char) b; break;
      case JSON_EOF: 
        return DS_MSG_STATUS_PARTIAL;

      case 'x': { /* format \xXX where X = hex nibble */
        uint32_t uc_b1, uc_b2;

        if ( (uc_b1 = hex_value( input.next() )) == 0xff ||
             (uc_b2 = hex_value( input.next() )) == 0xff )
          return DS_MSG_STATUS_BAD_JSON;
        *str++ = (char) ( ( uc_b1 << 8 ) | uc_b2 );
        break;
      }

      case 'u': { /* format \uXXXX where X = hex nibble */
        uint32_t uc_b1, uc_b2, uc_b3, uc_b4;

        if ( (uc_b1 = hex_value( input.next() )) == 0xff ||
             (uc_b2 = hex_value( input.next() )) == 0xff ||
             (uc_b3 = hex_value( input.next() )) == 0xff ||
             (uc_b4 = hex_value( input.next() )) == 0xff )
          return DS_MSG_STATUS_BAD_JSON;

        uint32_t uchar = ( uc_b1 << 12 ) | ( uc_b2 << 8 ) |
                         ( uc_b3 << 4 ) | uc_b4;
        if ( uchar <= 0x7f ) {
          *str++ = (char) uchar;
        }
        else if ( uchar <= 0x7ffU ) {
          if ( &str[ 1 ] == end ) {
            this->strval = (char *) input.extend( this->strval, sz, 16 );
            if ( this->strval == NULL )
              return DS_MSG_STATUS_ALLOC_FAIL;
            str = &this->strval[ sz ];
            sz += 16;
            end = &str[ 16 ];
          }
          *str++ = (char) ( 0xc0U | (   uchar            >> 6 ) );
          *str++ = (char) ( 0x80U |   ( uchar & 0x03fU ) );
        }
        else {
          if ( &str[ 2 ] >= end ) {
            this->strval = (char *) input.extend( this->strval, sz, 16 );
            if ( this->strval == NULL )
              return DS_MSG_STATUS_ALLOC_FAIL;
            str = &this->strval[ sz ];
            sz += 16;
            end = &str[ 16 ];
          }
          *str++ = (char) ( 0xe0U | (   uchar              >> 12 ) );
          *str++ = (char) ( 0x80U | ( ( uchar & 0x00fc0U ) >> 6 ) );
          *str++ = (char) ( 0x80U |   ( uchar & 0x0003fU ) );
        }
        break;
      }
    }
  }
}

RedisMsgStatus
RedisMsg::parse_number( JsonInput &input ) noexcept
{
  uint64_t integral = 0;
  int      c;
  bool     isneg;

  c = input.cur();
  if ( c == '-' ) {
    isneg = true;
    c = input.forward();
  }
  else {
    isneg = false;
  }
  while ( isdigit( c ) ) {
    integral = integral * 10 + ( c - '0' );
    c = input.forward(); /* eat digit */
  }
  this->type = DS_INTEGER_VALUE;
  this->len  = 0;
  if ( ! isneg )
    this->ival = (int64_t) integral;
  else
    this->ival = -(int64_t) integral;
  return DS_MSG_STATUS_OK;
}
