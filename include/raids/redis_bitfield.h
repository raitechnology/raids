#ifndef __rai_raids__redis_bitfield_h__
#define __rai_raids__redis_bitfield_h__

namespace rai {
namespace ds {

/* used for bitfield operator */
enum BitfieldOp   { OP_GET  = 0, OP_SET = 1, OP_INCRBY = 2 };
enum BitfieldOver { OV_WRAP = 0, OV_SAT = 1, OV_FAIL   = 2 };
struct Bitfield {
  uint64_t     type_off;   /* bit offset */
  int64_t      val;        /* set val or incr val */
  uint8_t      type_width; /* bit width */
  char         type_char;  /* I = int, U = unsigned */
  BitfieldOp   op;         /* get, set, incr */
  BitfieldOver overflow;   /* wrap, saturate, fail */
};

typedef struct Int72 {
   union {
     uint8_t b[ 9 ];
     uint64_t ival; /* presumes little endian, need to add swap for big */
   };
   Int72 &operator<<=( const int i ) {
     const int j = 64 - i;
     uint64_t tmp = ( this->ival >> j );
     if ( i < 8 )
       tmp |= ( (uint64_t) this->b[ 8 ] << i );
     this->b[ 8 ] = (uint8_t) tmp;
     this->ival <<= i;
     return *this;
   }
   Int72 &operator>>=( const int i ) {
     this->ival >>= i;
     this->ival |= (uint64_t) this->b[ 8 ] << ( 64 - i );
     this->b[ 8 ] >>= i;
     return *this;
   }
   Int72 &operator|=( const Int72 &v ) {
     this->b[ 8 ] |= v.b[ 8 ];
     this->ival |= v.ival;
     return *this;
   }
   Int72 &operator&=( const uint64_t v ) {
     this->b[ 8 ] = 0;
     this->ival &= v;
     return *this;
   }
   Int72 &operator&=( const Int72 &v ) {
     this->b[ 8 ] &= v.b[ 8 ];
     this->ival &= v.ival;
     return *this;
   }
   Int72 &operator=( const uint64_t v ) {
     this->b[ 8 ] = 0;
     this->ival   = v;
     return *this;
   }
   Int72 &operator=( const Int72 &v ) {
     this->b[ 8 ] = v.b[ 8 ];
     this->ival   = v.ival;
     return *this;
   }
   Int72 &operator|=( const uint64_t v ) {
     this->ival |= v;
     return *this;
   }
   Int72 operator~( void ) {
     Int72 tmp = *this;
     tmp.ival = ~tmp.ival;
     tmp.b[ 8 ] = ~tmp.b[ 8 ];
     return tmp;
   }
} int72_t;

}
}
#endif
