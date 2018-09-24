#ifndef __rai_raids__decimal_h__
#define __rai_raids__decimal_h__

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t Decimal64Storage;

void dec64_itod( Decimal64Storage *fp,  int i );
void dec64_ftod( Decimal64Storage *fp,  double f );
void dec64_from_string( Decimal64Storage *fp,  const char *str );
void dec64_zero( Decimal64Storage *fp );
size_t dec64_to_string( const Decimal64Storage *fp,  char *str );
void dec64_sum( Decimal64Storage *out,  const Decimal64Storage *l,
                const Decimal64Storage *r );
void dec64_mul( Decimal64Storage *out,  const Decimal64Storage *l,
                const Decimal64Storage *r );
int dec64_eq( const Decimal64Storage *l, const Decimal64Storage *r );
int dec64_lt( const Decimal64Storage *l, const Decimal64Storage *r );
int dec64_gt( const Decimal64Storage *l, const Decimal64Storage *r );

typedef struct { uint64_t n[ 2 ]; } Decimal128Storage;

void dec128_itod( Decimal128Storage *fp,  int i );
void dec128_ftod( Decimal128Storage *fp,  double f );
void dec128_from_string( Decimal128Storage *fp,  const char *str );
void dec128_zero( Decimal128Storage *fp );
size_t dec128_to_string( const Decimal128Storage *fp,  char *str );
void dec128_mul( Decimal128Storage *out,  const Decimal128Storage *l,
                 const Decimal128Storage *r );
void dec128_sum( Decimal128Storage *out,  const Decimal128Storage *l,
                const Decimal128Storage *r );
int dec128_eq( const Decimal128Storage *l, const Decimal128Storage *r );
int dec128_lt( const Decimal128Storage *l, const Decimal128Storage *r );
int dec128_gt( const Decimal128Storage *l, const Decimal128Storage *r );

#ifdef __cplusplus
}

namespace rai {
namespace ds {

static inline char *
null_terminate( char *buf,  size_t buflen,  const char *str,  size_t strlen ) {
  if ( strlen > buflen - 1 ) strlen = buflen - 1;
  ::memcpy( buf, str, strlen );
  buf[ strlen ] = '\0';
  return buf;
}

struct Decimal64 {
  Decimal64Storage fp;

  static Decimal64 parse( const char *str ) {
    Decimal64 x; dec64_from_string( &x.fp, str ); return x;
  }
  static Decimal64 parse_len( const char *str,  size_t len ) {
    char buf[ 64 ];
    return Decimal64::parse( null_terminate( buf, sizeof( buf ), str, len ) );
  }
  static Decimal64 itod( int i ) {
    Decimal64 x; x = i; return x;
  }
  void zero( void ) {
    dec64_zero( &this->fp );
  }
  size_t to_string( char *str ) {
    return dec64_to_string( &this->fp, str );
  }
  void from_string( const char *str,  size_t len ) {
    char buf[ 64 ];
    dec64_from_string( &this->fp,
      null_terminate( buf, sizeof( buf ), str, len ) );
  }
  Decimal64& operator =( double f ) {
    dec64_ftod( &this->fp, f );
    return *this;
  }
  Decimal64& operator =( int i ) {
    dec64_itod( &this->fp, i );
    return *this;
  }
  Decimal64& operator +=( const Decimal64 &f ) {
    dec64_sum( &this->fp, &this->fp, &f.fp );
    return *this;
  }
  Decimal64 operator +( const Decimal64 &f ) const {
    Decimal64 tmp;
    dec64_sum( &tmp.fp, &this->fp, &f.fp );
    return tmp;
  }
  Decimal64& operator *=( const Decimal64 &f ) {
    dec64_mul( &this->fp, &this->fp, &f.fp );
    return *this;
  }
  Decimal64 operator *( const Decimal64 &f ) const {
    Decimal64 tmp;
    dec64_mul( &tmp.fp, &this->fp, &f.fp );
    return tmp;
  }
  bool operator ==( const Decimal64 &f ) const {
    return dec64_eq( &this->fp, &f.fp );
  }
  bool operator !=( const Decimal64 &f ) const {
    return ! dec64_eq( &this->fp, &f.fp );
  }
  bool operator <( const Decimal64 &f ) const {
    return dec64_lt( &this->fp, &f.fp );
  }
  bool operator >( const Decimal64 &f ) const {
    return dec64_gt( &this->fp, &f.fp );
  }
  bool operator >=( const Decimal64 &f ) const {
    return ! dec64_lt( &this->fp, &f.fp );
  }
  bool operator <=( const Decimal64 &f ) const {
    return ! dec64_gt( &this->fp, &f.fp );
  }
};

struct Decimal128 {
  Decimal128Storage fp;

  static Decimal128 parse( const char *str ) {
    Decimal128 x; dec128_from_string( &x.fp, str ); return x;
  }
  static Decimal128 parse_len( const char *str,  size_t len ) {
    char buf[ 64 ];
    return Decimal128::parse( null_terminate( buf, sizeof( buf ), str, len ) );
  }
  void zero( void ) {
    dec128_zero( &this->fp );
  }
  size_t to_string( char *str ) {
    return dec128_to_string( &this->fp, str );
  }
  void from_string( const char *str,  size_t len ) {
    char buf[ 64 ];
    dec128_from_string( &this->fp,
      null_terminate( buf, sizeof( buf ), str, len ) );
  }
  Decimal128& operator =( int i ) {
    dec128_itod( &this->fp, i );
    return *this;
  }
  Decimal128& operator =( double f ) {
    dec128_itod( &this->fp, f );
    return *this;
  }
  Decimal128& operator +=( const Decimal128 &f ) {
    dec128_sum( &this->fp, &this->fp, &f.fp );
    return *this;
  }
  Decimal128 operator +( const Decimal128 &f ) const {
    Decimal128 tmp;
    dec128_sum( &tmp.fp, &this->fp, &f.fp );
    return tmp;
  }
  Decimal128& operator *=( const Decimal128 &f ) {
    dec128_mul( &this->fp, &this->fp, &f.fp );
    return *this;
  }
  Decimal128 operator *( const Decimal128 &f ) const {
    Decimal128 tmp;
    dec128_mul( &tmp.fp, &this->fp, &f.fp );
    return tmp;
  }
  bool operator ==( const Decimal128 &f ) const {
    return dec128_eq( &this->fp, &f.fp );
  }
  bool operator !=( const Decimal128 &f ) const {
    return ! dec128_eq( &this->fp, &f.fp );
  }
  bool operator <( const Decimal128 &f ) const {
    return dec128_lt( &this->fp, &f.fp );
  }
  bool operator >( const Decimal128 &f ) const {
    return dec128_gt( &this->fp, &f.fp );
  }
  bool operator >=( const Decimal128 &f ) const {
    return ! dec128_gt( &this->fp, &f.fp );
  }
  bool operator <=( const Decimal128 &f ) const {
    return ! dec128_lt( &this->fp, &f.fp );
  }
};

}
}
#endif
#endif
