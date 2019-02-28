#ifndef __rai_raids__decimal_h__
#define __rai_raids__decimal_h__

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t Dec64Store;

void dec64_itod( Dec64Store *fp,  int i );
void dec64_ftod( Dec64Store *fp,  double f );
void dec64_from_string( Dec64Store *fp,  const char *str );
void dec64_zero( Dec64Store *fp );
size_t dec64_to_string( const Dec64Store *fp,  char *str );
void dec64_sum( Dec64Store *out,  const Dec64Store *l,
                const Dec64Store *r );
void dec64_mul( Dec64Store *out,  const Dec64Store *l,
                const Dec64Store *r );
int dec64_eq( const Dec64Store *l, const Dec64Store *r );
int dec64_lt( const Dec64Store *l, const Dec64Store *r );
int dec64_gt( const Dec64Store *l, const Dec64Store *r );

typedef struct { uint64_t n[ 2 ]; } Dec128Store;

void dec128_itod( Dec128Store *fp,  int i );
void dec128_ftod( Dec128Store *fp,  double f );
void dec128_from_string( Dec128Store *fp,  const char *str );
void dec128_zero( Dec128Store *fp );
size_t dec128_to_string( const Dec128Store *fp,  char *str );
void dec128_mul( Dec128Store *out,  const Dec128Store *l,
                 const Dec128Store *r );
void dec128_sum( Dec128Store *out,  const Dec128Store *l,
                const Dec128Store *r );
int dec128_eq( const Dec128Store *l, const Dec128Store *r );
int dec128_lt( const Dec128Store *l, const Dec128Store *r );
int dec128_gt( const Dec128Store *l, const Dec128Store *r );

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
  Dec64Store fp;

  static Decimal64 parse( const char *str ) {
    Decimal64 x; dec64_from_string( &x.fp, str ); return x;
  }
  static Decimal64 parse_len( const char *str,  size_t len ) {
    char buf[ 64 ];
    return Decimal64::parse( null_terminate( buf, sizeof( buf ), str, len ) );
  }
  static Decimal64 itod( int i ) {
    Decimal64 x; dec64_itod( &x.fp, i ); return x;
    /*Decimal64 x; x = i; return x;*/
  }
  static Decimal64 ftod( double f ) {
    Decimal64 x; dec64_ftod( &x.fp, f ); return x;
    /*Decimal64 x; x = i; return x;*/
  }
  static void zero( Decimal64 *d ) { d->zero(); }
  static void zero( double *d )    { *d = 0; }
  static void zero( uint64_t *d )  { *d = 0; }
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
#if 0
  Decimal64& operator =( double f ) {
    dec64_ftod( &this->fp, f );
    return *this;
  }
  Decimal64& operator =( int i ) {
    dec64_itod( &this->fp, i );
    return *this;
  }
#endif
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
  Dec128Store fp;

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
  static Decimal128 ftod( double f ) {
    Decimal128 x; dec128_ftod( &x.fp, f ); return x;
  }
#if 0
  Decimal128& operator =( int i ) {
    dec128_itod( &this->fp, i );
    return *this;
  }
  Decimal128& operator =( double f ) {
    dec128_itod( &this->fp, f );
    return *this;
  }
#endif
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
