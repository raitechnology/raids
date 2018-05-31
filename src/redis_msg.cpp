#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <raids/redis_msg.h>

using namespace rai;
using namespace ds;
using namespace kv;

const char *
rai::ds::redis_msg_status_string( RedisMsgStatus status ) {
  switch ( status ) {
    case REDIS_MSG_OK:         return "OK";
    case REDIS_MSG_BAD_TYPE:   return "BAD_TYPE";
    case REDIS_MSG_PARTIAL:    return "PARTIAL";
    case REDIS_MSG_ALLOC_FAIL: return "ALLOC_FAIL";
    case REDIS_MSG_BAD_JSON:   return "BAD_JSON";
  }
  return "UNKNOWN";
}

const char *
rai::ds::redis_msg_status_description( RedisMsgStatus status ) {
  switch ( status ) {
    case REDIS_MSG_OK:         return "OK";
    case REDIS_MSG_BAD_TYPE:   return "Message decoding error, bad type char";
    case REDIS_MSG_PARTIAL:    return "Partial message";
    case REDIS_MSG_ALLOC_FAIL: return "Alloc failed";
    case REDIS_MSG_BAD_JSON:   return "Unable to parse JSON message";
  }
  return "Unknown msg status";
}

template<class T>
static inline uint32_t data_type_mask( T t ) {
  uint8_t x = (uint8_t) t - (uint8_t) RedisMsg::BULK_STRING;
  return x < 32 ? ( 1U << x ) : 0U;
}

static inline bool is_valid( uint32_t tb ) {
  static const uint32_t valid_bits = DTBit( RedisMsg::SIMPLE_STRING ) |
                                     DTBit( RedisMsg::ERROR_STRING ) |
                                     DTBit( RedisMsg::INTEGER_VALUE ) |
                                     DTBit( RedisMsg::BULK_STRING ) |
                                     DTBit( RedisMsg::BULK_ARRAY );
  return ( valid_bits & tb ) != 0;
}

static inline bool is_simple_type( uint32_t tb ) {
  static const uint32_t valid_bits = DTBit( RedisMsg::SIMPLE_STRING ) |
                                     DTBit( RedisMsg::ERROR_STRING );
  return ( valid_bits & tb ) != 0;
}

static inline bool is_int_type( uint32_t tb ) {
  static const uint32_t valid_bits = DTBit( RedisMsg::INTEGER_VALUE );
  return ( valid_bits & tb ) != 0;
}

static inline bool is_bulk_string( uint32_t tb ) {
  static const uint32_t valid_bits = DTBit( RedisMsg::BULK_STRING );
  return ( valid_bits & tb ) != 0;
}

RedisMsgStatus
RedisMsg::pack( void *buf,  size_t &buflen )
{
  char  * ptr = (char *) buf;
  size_t  i;
  const uint32_t type_bit = data_type_mask<DataType>( this->type );

  if ( ! is_valid( type_bit ) )
    return REDIS_MSG_BAD_TYPE;

  if ( 32 >= buflen ) /* int type + 23 digit number + 2 trail = 26 */
    return REDIS_MSG_PARTIAL;

  ptr[ 0 ] = (char) this->type;
  if ( is_simple_type( type_bit ) ) {
    i = 1 + this->len;
    if ( i + 2 >= buflen )
      return REDIS_MSG_PARTIAL;
    ::memcpy( &ptr[ 1 ], this->strval, this->len );
  }
  else if ( is_int_type( type_bit ) ) {
    i = 1 + kv::int64_to_string( this->ival, &ptr[ 1 ] );
  }
  else {
    i = 1 + kv::int64_to_string( this->len, &ptr[ 1 ] );
    if ( is_bulk_string( type_bit ) ) {
      if ( this->len >= 0 ) {
        ptr[ i ] = '\r';
        ptr[ i + 1 ] = '\n';
        i += 2;
        if ( i + this->len + 2 >= buflen )
          return REDIS_MSG_PARTIAL;
        ::memcpy( &ptr[ i ], this->strval, this->len );
        i += this->len;
      }
    }
    else {
      if ( this->len >= 0 ) {
        ptr[ i ] = '\r';
        ptr[ i + 1 ] = '\n';
        i += 2;
        for ( size_t k = 0; k < (size_t) this->len; k++ ) {
          size_t tmp = buflen - i;
          RedisMsgStatus stat = this->array[ k ].pack( &ptr[ i ], tmp );
          if ( stat != REDIS_MSG_OK )
            return stat;
          i += tmp;
        }
        goto skip_trailing_crnl;
      }
    }
  }
  ptr[ i ] = '\r';
  ptr[ i + 1 ] = '\n';
  i += 2;
skip_trailing_crnl:;
  buflen = i;
  return REDIS_MSG_OK;
}

bool
RedisMsg::alloc_array( ScratchMem &wrk,  int64_t sz )
{
  this->type  = BULK_ARRAY;
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

RedisMsgStatus
RedisMsg::split( ScratchMem &wrk )
{
  char * ptr = this->strval,
       * end = &this->strval[ this->len ];
  size_t cnt = 1;

  while ( (ptr = (char *) ::memchr( ptr, ' ', end - ptr )) != NULL ) {
    ptr++;
    cnt++;
  }
  RedisMsg *tmp = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * cnt );
  if ( tmp == NULL )
    return REDIS_MSG_ALLOC_FAIL;
  if ( cnt > 1 )
    ptr = (char *) ::memchr( this->strval, ' ', end - this->strval );
  else
    ptr = end;
  tmp[ 0 ].type   = BULK_STRING;
  tmp[ 0 ].len    = ptr - this->strval;
  tmp[ 0 ].strval = this->strval;
  if ( cnt > 1 ) {
    cnt = 1;
    for (;;) {
      while ( ptr < end && *ptr == ' ' )
        ptr++;
      if ( ptr == end )
        break;
      tmp[ cnt ].type   = BULK_STRING;
      tmp[ cnt ].strval = ptr;
      ptr = (char *) ::memchr( ptr, ' ', end - ptr );
      if ( ptr == NULL )
        ptr = end;
      tmp[ cnt ].len = ptr - tmp[ cnt ].strval;
      cnt++;
    }
  }
  this->type  = BULK_ARRAY;
  this->len   = cnt;
  this->array = tmp;
  return REDIS_MSG_OK;
}

RedisMsgStatus
RedisMsg::unpack( void *buf,  size_t &buflen,  ScratchMem &wrk )
{
  char  * ptr = (char *) buf, /* buflen must be at least 1 */
        * eol = (char *) ::memchr( &ptr[ 1 ], '\n', buflen - 1 );
  size_t  i, j;

  if ( eol == NULL )
    return REDIS_MSG_PARTIAL;

  i = eol - &ptr[ 1 ];
  j = i + 2;
  if ( ptr[ i ] == '\r' )
    i--;

  const uint32_t type_bit = data_type_mask<char>( ptr[ 0 ] );
  if ( ! is_valid( type_bit ) ) {
    /* inline command */
    this->type   = SIMPLE_STRING;
    this->len    = i + 1;
    this->strval = ptr;
    buflen = j;
    return this->split( wrk );
  }

  this->type = (DataType) ptr[ 0 ];

  if ( is_simple_type( type_bit ) ) {
    this->len = i;
    this->strval = &ptr[ 1 ];
  }
  else if ( is_int_type( type_bit ) ) {
    this->len  = 0;
    this->ival = kv::string_to_int64( &ptr[ 1 ], i );
  }
  else {
    this->len = kv::string_to_int64( &ptr[ 1 ], i );
    if ( is_bulk_string( type_bit ) ) {
      if ( this->len > 0 ) {
        this->strval = &ptr[ j ];
        j += this->len;
        if ( j > buflen )
          return REDIS_MSG_PARTIAL;
      }
      else
        this->strval = NULL;
      if ( this->len >= 0 ) {
        if ( j < buflen && ptr[ j ] == '\r' )
          j++;
        if ( j < buflen && ptr[ j ] == '\n' )
          j++;
      }
    }
    else {
      if ( this->len > 0 ) {
        this->array = (RedisMsg *) wrk.alloc( sizeof( RedisMsg ) * this->len );
        if ( this->array == NULL )
          return REDIS_MSG_ALLOC_FAIL;
        for ( size_t k = 0; k < (size_t) this->len; k++ ) {
          size_t tmp = buflen - j;
          if ( tmp == 0 )
            return REDIS_MSG_PARTIAL;
          RedisMsgStatus stat = this->array[ k ].unpack( &ptr[ j ], tmp, wrk );
          if ( stat != REDIS_MSG_OK )
            return stat;
          j += tmp;
        }
      }
      else
        this->array = NULL;
    }
  }
  buflen = j;
  return REDIS_MSG_OK;
}

int
RedisMsg::match_arg( int n,  const char *str,  size_t sz,  ... )
{
  int64_t i, start, end;
  int k = 0;
  va_list args;
  if ( n < 0 ) {
    start = -n;
    end   = this->len;
  }
  else {
    start = n;
    end   = n + 1;
    if ( end > this->len )
      end = this->len;
  }     
  va_start( args, sz );
  for ( k = 1; ; k++ ) {
    for ( i = start; i < end; i++ ) {
      if ( this->array[ i ].is_string() ) {
	if ( (size_t) this->array[ i ].len == sz &&
	     ::strncasecmp( str, this->array[ i ].strval, sz ) == 0 )
	  goto break_loop;
      }
    }
    str = va_arg( args, const char * );
    if ( str == NULL ) {
      k = 0;
      goto break_loop;
    }
    sz = va_arg( args, size_t );
  }
break_loop:;
  va_end( args );
  return k;
}

/* doesn't escape strings, uses different quote styles for various strings:
 *   " for simple, ` for error, ' for bulk strings
 * null = -1 sized array
 * nil  = -1 sized string
 */
RedisMsgStatus
RedisMsg::to_json( char *buf,  size_t &buflen ) const
{
  size_t         sz = buflen;
  RedisMsgStatus x;
  char           q;

  switch ( this->type ) {
    case SIMPLE_STRING: q = '"';
      if ( 0 ) {
    case ERROR_STRING:  q = '`';
        if ( 0 ) {
    case BULK_STRING:   q = '\'';
      } }
      if ( this->len >= 0 ) {
        if ( (size_t) this->len + 3 > sz )
          return REDIS_MSG_PARTIAL;
        ::memcpy( &buf[ 1 ], this->strval, this->len );
        buf[ 0 ] = q;
        buf[ 1 + this->len ] = q;
        buf[ 2 + this->len ] = '\0';
        buflen = this->len + 2;
      }
      else {
        if ( 4 > sz )
          return REDIS_MSG_PARTIAL;
        ::strcpy( buf, "nil" );
        buflen = 3;
      }
      return REDIS_MSG_OK;

    case INTEGER_VALUE:
      if ( 23 > sz )
        return REDIS_MSG_PARTIAL;
      buflen = kv::int64_to_string( this->ival, buf );
      return REDIS_MSG_OK;

    case BULK_ARRAY:
      if ( this->len >= 0 ) {
        if ( 3 > sz )
          return REDIS_MSG_PARTIAL;
        size_t j = 1;
        buf[ 0 ] = '[';
        if ( this->len > 0 ) {
          size_t z = sz;
          if ( (x = this->array[ 0 ].to_json( &buf[ j ], z )) != REDIS_MSG_OK )
            return x;
          j  += z;
          sz -= z;
        }
        for ( size_t i = 1; i < (size_t) this->len; i++ ) {
          if ( 1 > sz )
            return REDIS_MSG_PARTIAL;
          buf[ j++ ] = ',';
          sz -= 1;
          size_t z = sz;
          if ( (x = this->array[ i ].to_json( &buf[ j ], z )) != REDIS_MSG_OK )
            return x;
          j  += z;
          sz -= z;
        }
        if ( 2 > sz )
          return REDIS_MSG_PARTIAL;
        buf[ j ] = ']';
        buf[ j + 1 ] = '\0';
        buflen = j + 1;
      }
      else {
        if ( 5 > sz )
          return REDIS_MSG_PARTIAL;
        ::strcpy( buf, "null" );
        buflen = 4;
      }
      return REDIS_MSG_OK;

    default:
      break;
  }
  return REDIS_MSG_BAD_TYPE;
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
  bool match( char c1,  char c2,  char c3,  char c4,  char c5 );
  int  eat_white( void );

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
  void * extend( void *obj,  size_t oldsz,  size_t newsz ) {
    void * p = this->wrk.alloc( newsz );
    if ( p != NULL )
      ::memcpy( p, obj, oldsz );
    return p;
  }
};
}
}

RedisMsgStatus
RedisMsg::unpack_json( const char *json,  size_t &len,  ScratchMem &wrk )
{
  JsonInput input( wrk, json, 0, len );
  RedisMsgStatus status = this->parse_json( input );
  if ( status == REDIS_MSG_OK ) {
    len = input.offset;
    return REDIS_MSG_OK;
  }
  return status;
}

int
JsonInput::eat_white( void )
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
JsonInput::match( char c1,  char c2,  char c3,  char c4,  char c5 )
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
RedisMsg::parse_json( JsonInput &input )
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
    case JSON_EOF: return REDIS_MSG_PARTIAL;
    case 't': if ( input.match( 't', 'r', 'u', 'e', 0 ) ) {
                this->type = INTEGER_VALUE;
                this->len  = 0;
                this->ival = 1;
                return REDIS_MSG_OK;
              if ( 0 ) {
    case 'f': if ( input.match( 'f', 'a', 'l', 's', 'e' ) ) {
                this->type = INTEGER_VALUE;
                this->len  = 0;
                this->ival = 0;
                return REDIS_MSG_OK;
              if ( 0 ) {
    case 'n': if ( input.match( 'n', 'u', 'l', 'l', 0 ) ) {
                this->type  = BULK_ARRAY;
                this->len   = -1;
                this->array = NULL;
                return REDIS_MSG_OK;
              }
              else if ( input.match( 'n', 'i', 'l', 0, 0 ) ) {
                this->type   = BULK_STRING;
                this->len    = -1;
                this->strval = NULL;
                return REDIS_MSG_OK;
              }
              } } } }
    default:
      return REDIS_MSG_BAD_JSON;
  }
}

RedisMsgStatus
RedisMsg::parse_object( JsonInput &input )
{
  /* no way of representing objects */
  return REDIS_MSG_BAD_JSON;
}

RedisMsgStatus
RedisMsg::parse_array( JsonInput &input )
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
    if ( (status = value.parse_json( input )) != REDIS_MSG_OK )
      return status;

    if ( tos == 0 || &val[ tos ][ i ] == end[ tos ] ) {
      size_t newsz = ( sz + 2 ) * 3 / 2;
      tos++;
      if ( tos == sizeof( val ) / sizeof( val[ 0 ] ) )
        return REDIS_MSG_BAD_JSON;
      val[ tos ] = (RedisMsg *) input.alloc( sizeof( RedisMsg ) * newsz );
      if ( val[ tos ] == NULL )
        return REDIS_MSG_ALLOC_FAIL;
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
      return REDIS_MSG_PARTIAL;
    return REDIS_MSG_BAD_JSON;
  }
  input.next(); /* eat ']' */

  if ( tos > 0 ) {
    if ( tos == 1 ) {
      this->type  = BULK_ARRAY;
      this->len   = i;
      this->array = val[ 1 ];
    }
    else {
      sz = i;
      for ( j = 1; j < tos; j++ )
        sz += end[ j ] - val[ j ];
      this->type  = BULK_ARRAY;
      this->len   = sz;
      this->array = (RedisMsg *) input.alloc( sizeof( RedisMsg ) * sz );
      if ( this->array == NULL )
        return REDIS_MSG_ALLOC_FAIL;
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
    this->type  = BULK_ARRAY;
    this->len   = 0;
    this->array = NULL;
  }
  return REDIS_MSG_OK;
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
RedisMsg::parse_string( JsonInput &input )
{
  size_t sz = 8;
  char * str,
       * end;
  int    quote;

  str = this->strval = (char *) input.alloc( 8 );
  if ( str == NULL )
    return REDIS_MSG_ALLOC_FAIL;
  end = &str[ 8 ];
  quote = input.next(); /* eat '"' */
  for (;;) {
    int c = input.next();
    if ( c == JSON_EOF )
      return REDIS_MSG_PARTIAL;
    if ( str == end ) {
      this->strval = (char *) input.extend( this->strval, sz, 16 );
      if ( this->strval == NULL )
        return REDIS_MSG_ALLOC_FAIL;
      str = &this->strval[ sz ];
      sz += 16;
      end = &str[ 16 ];
    }
    if ( c == quote ) {
      *str = '\0';
      if ( quote == '\'' )
        this->type = BULK_STRING;
      else if ( quote == '"' )
        this->type = SIMPLE_STRING;
      else
        this->type = ERROR_STRING;
      this->len  = (int64_t) ( str - this->strval );
      return REDIS_MSG_OK;
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
        return REDIS_MSG_PARTIAL;

      case 'u': { /* format \uXXXX where X = hex nibble */
        uint32_t uc_b1, uc_b2, uc_b3, uc_b4;

        if ( (uc_b1 = hex_value( input.next() )) == 0xff ||
             (uc_b2 = hex_value( input.next() )) == 0xff ||
             (uc_b3 = hex_value( input.next() )) == 0xff ||
             (uc_b4 = hex_value( input.next() )) == 0xff )
          return REDIS_MSG_BAD_JSON;

        uint32_t uchar = ( uc_b1 << 12 ) | ( uc_b2 << 8 ) |
                         ( uc_b3 << 4 ) | uc_b4;
        if ( uchar <= 0x7f ) {
          *str++ = (char) uchar;
        }
        else if ( uchar <= 0x7ffU ) {
          if ( &str[ 1 ] == end ) {
            this->strval = (char *) input.extend( this->strval, sz, 16 );
            if ( this->strval == NULL )
              return REDIS_MSG_ALLOC_FAIL;
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
              return REDIS_MSG_ALLOC_FAIL;
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
RedisMsg::parse_number( JsonInput &input )
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
  this->type = INTEGER_VALUE;
  this->len  = 0;
  if ( ! isneg )
    this->ival = (int64_t) integral;
  else
    this->ival = -(int64_t) integral;
  return REDIS_MSG_OK;
}
