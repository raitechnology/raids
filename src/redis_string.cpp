#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_bitfield.h>
#include <raimd/decimal.h>
#include <raimd/md_types.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

enum {
  HAS_EXPIRE_NS    = 1, /* SET flags: EX expire, NX not exist, XX must exist */
  K_MUST_NOT_EXIST = 2,
  K_MUST_EXIST     = 4
};

ExecStatus
RedisExec::exec_append( EvKeyCtx &ctx ) noexcept
{
  void       * data    = NULL;
  const char * value;
  uint64_t     data_sz = 0;
  size_t       valuelen;

  if ( ! this->msg.get_arg( 2, value, valuelen ) ) /* APPEND KEY VALUE */
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.get_size( data_sz );
      if ( ctx.kstatus == KEY_OK ) {
    case KEY_IS_NEW:
        ctx.ival    = data_sz + valuelen;
        ctx.kstatus = this->kctx.resize( &data, ctx.ival, true );
        if ( ctx.kstatus == KEY_OK ) {
          ::memcpy( &((uint8_t *) data)[ data_sz ], value, valuelen );
          ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
          return EXEC_SEND_INT;
        }
      }
    /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_bitcount( EvKeyCtx &ctx ) noexcept
{
  int64_t start = 0, end = -1;

  /* BITCOUNT KEY [start end] */
  if ( this->argc > 2 && ! this->msg.get_arg( 2, start ) )
    return ERR_BAD_ARGS;
  if ( this->argc > 3 && ! this->msg.get_arg( 3, end ) )
    return ERR_BAD_ARGS;

  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK: {
      void *data;
      uint64_t size;

      ctx.kstatus = this->kctx.value( &data, size );
      if ( size == 0 )
        return EXEC_SEND_ZERO;

      if ( ctx.kstatus == KEY_OK ) {
        size_t i, len = 0;
        int64_t start_off, end_off, cnt = 0;
        uint64_t tail = 0;

        start_off = ( start < 0 ) ? (int64_t) size + start : start;
        end_off   = ( end   < 0 ) ? (int64_t) size + end   : end;
        start_off = min<int64_t>( max<int64_t>( start_off, 0 ), size - 1 );
        end_off   = max<int64_t>( min<int64_t>( end_off, size - 1 ), 0 );
        if ( end_off >= start_off )
          len = ( end_off + 1 ) - start_off;

        uint8_t *p = &((uint8_t *) data)[ start_off ];
        for ( i = 0; i + 8 < len; i += 8 )
          cnt += __builtin_popcountl( *(uint64_t *) (void *) &p[ i ] );
        p = &p[ i ];
        i = len - i;
        if ( ( i & 4 ) != 0 ) {
          tail = *(uint32_t *) (void *) p;
          p += 4;
        }
        if ( ( i & 2 ) != 0 ) {
          tail = ( tail << 16 ) | (uint64_t) ( *(uint16_t *) (void *) p );
          p += 2;
        }
        if ( ( i & 1 ) != 0 )
          tail = ( tail << 8 ) | (uint64_t) ( *(uint8_t *) (void *) p );
        cnt += __builtin_popcountl( tail );
        ctx.ival = cnt;

        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK )
          return EXEC_SEND_INT;
      }
      /* FALLTHRU */
    }
    /* fall through */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_bitfield( EvKeyCtx &ctx ) noexcept
{
  Bitfield     * bf;
  void         * data;
  const char   * type,
               * off;
  size_t         type_sz,
                 off_sz;
  uint64_t       type_off,
                 max_off = 0,
                 size,
                 old_size;
  size_t         i,
                 k = 0;
  int64_t        val;
  char           type_char;
  uint8_t        type_width;
  BitfieldOp     op;
  BitfieldOver   overflow; /* OV_WRAP = 0, OV_SAT = 1, OV_FAIL = 2 */

  if ( this->argc < 3 )
    return ERR_BAD_ARGS;
  bf = (Bitfield *) this->strm.alloc_temp( this->argc / 3 * sizeof( bf[ 0 ] ) );
  if ( bf == NULL )
    return ERR_ALLOC_FAIL;
  /* split args into bf[] array */
  for ( i = 2; ; ) {
    /* [overflow (wrap|sat|fail)] incrby type off val */
    if ( this->msg.match_arg( i, MARG( "overflow" ), NULL ) == 1 ) {
      int over = this->msg.match_arg( i + 1, MARG( "wrap" ),
                                             MARG( "sat" ),
                                             MARG( "fail" ), NULL );
      if ( over == 0 )
        return ERR_BAD_ARGS;
      overflow = (BitfieldOver) ( over - 1 );
      i += 2;
    }
    else {
      overflow = OV_WRAP; /* default */
    }
    /* all formats have a type (ex: u8, i3) and offset (ex: #1, 100) */
    if ( ! this->msg.get_arg( i+1, type, type_sz ) ||
         ! this->msg.get_arg( i+2, off, off_sz ) ||
           type_sz < 2 || off_sz < 1 )
      return ERR_BAD_ARGS;
    /* parse type, ex: u8 */
    type_char = toupper( type[ 0 ] ); /* I or U */
    this->mstatus = (RedisMsgStatus)
                    string_to_int( &type[ 1 ], type_sz - 1, val );
    if ( this->mstatus != DS_MSG_STATUS_OK )
      return ERR_BAD_ARGS;
    if ( type_char == 'I' ) {
      if ( val < 2 || val > 64 )
        return ERR_BAD_ARGS;
    }
    else if ( type_char == 'U' ) {
      if ( val < 1 || val > 63 )
        return ERR_BAD_ARGS;
    }
    else {
      return ERR_BAD_ARGS;
    }
    type_width = (uint8_t) val;
    /* offset may be bit num or #element num */
    if ( off[ 0 ] == '#' )
      this->mstatus = (RedisMsgStatus)
                      string_to_int( &off[ 1 ], off_sz - 1, val );
    else
      this->mstatus = (RedisMsgStatus)
                      string_to_int( off, off_sz, val );
    if ( this->mstatus != DS_MSG_STATUS_OK )
      return ERR_BAD_ARGS;
    type_off = (uint64_t) val;
    if ( off[ 0 ] == '#' )
      type_off *= type_width;
    switch ( this->msg.match_arg( i, MARG( "get" ),
                                     MARG( "set" ),
                                     MARG( "incrby" ), NULL ) ) {
      case 1: /* bitfield key [get type off] */
        op = OP_GET;
        val = 0;
        i += 3;
        break;
      case 2: /* bitfield key [set type off val] */
        op = OP_SET;
        if ( ! this->msg.get_arg( i + 3, val ) )
          return ERR_BAD_ARGS;
        i += 4;
        break;
      case 3: /* bitfield key [incrby type off val] */
        if ( ! this->msg.get_arg( i + 3, val ) )
          return ERR_BAD_ARGS;
        op = OP_INCRBY;
        i += 4;
        /* incrby type off val [overflow (wrap|sat|fail)] */
        if ( this->msg.match_arg( i, MARG( "overflow" ), NULL ) == 1 ) {
          int over = this->msg.match_arg( i + 1, MARG( "wrap" ),
                                                 MARG( "sat" ),
                                                 MARG( "fail" ), NULL );
          if ( over == 0 )
            return ERR_BAD_ARGS;
          overflow = (BitfieldOver) ( over - 1 );
          i += 2;
        }
        break;
      default: return ERR_BAD_ARGS;
    }
    bf[ k ].type_off   = type_off;
    bf[ k ].val        = val;
    bf[ k ].type_width = type_width;
    bf[ k ].type_char  = type_char;
    bf[ k ].op         = op;
    bf[ k ].overflow   = overflow;

    /* calc max offset for resizing the bit array */
    type_off += type_width;
    if ( op != OP_GET && type_off > max_off )
      max_off = type_off;
    k++;
    if ( i == this->argc )
      break;
  }
  if ( max_off > 0 ) { /* write access */
    max_off  = align<uint64_t>( max_off, 8 );
    size     = max_off / 8;
    old_size = 0;
    switch ( this->get_key_write( ctx, MD_STRING ) ) {
      case KEY_OK:
        ctx.kstatus = this->kctx.get_size( old_size );
        if ( ctx.kstatus == KEY_OK ) {
	  if ( old_size >= size )
	    size = old_size;
      case KEY_IS_NEW:
          ctx.kstatus = this->kctx.resize( &data, size );
	  if ( old_size < size )
	    ::memset( &((uint8_t *) data)[ old_size ], 0, size - old_size );
          if ( ctx.kstatus == KEY_OK ) {
            ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
	    break;
          }
        }
        /* FALLTHRU */
      default:            return ERR_KV_STATUS;
      case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    }
  }
  else { /* read access */
    switch ( this->get_key_read( ctx, MD_STRING ) ) {
      case KEY_OK:
        ctx.kstatus = this->kctx.value( &data, size );
        if ( ctx.kstatus == KEY_OK )
          break;
        /* FALLTHRU */
      default:            return ERR_KV_STATUS;
      case KEY_NOT_FOUND: data = NULL; size = 0; break;
      case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    }
  }
  size_t sz  = 32 + k * 32;
  char * str = this->strm.alloc( sz );
  if ( str == NULL )
    return ERR_ALLOC_FAIL;

  str[ 0 ] = '*';
  sz = 1 + uint_to_str( k, &str[ 1 ] );
  sz = crlf( str, sz );
  for ( i = 0; i < k; i++ ) {
    const uint8_t  width = bf[ i ].type_width;
    const char     tchar = bf[ i ].type_char;
    const uint64_t mask  = width == 64 ? (uint64_t) -1 :
                           ( (uint64_t) 1 << width ) - 1;
    const uint64_t signb = ( mask >> 1 ) + 1;
    uint64_t       off   = bf[ i ].type_off / 8,
                   start = off;
    const int      shift = bf[ i ].type_off % 8;
    uint8_t        w     = 0;
    bool           fail  = false;
    int            j;
    int72_t        upd_val, /* need 72 bits, for 64bits << shift, shift:0->7 */
                   old_val,
                   msk_val;
    upd_val = 0;
    for ( j = 0; off < size; ) {
      upd_val.b[ j++ ] = ((uint8_t *) data)[ off++ ];
      if ( (w += 8) >= width + shift ) /* if have enough bits for iN type */
        break;
    }
    /* the current value at bit offset */
    old_val   = upd_val;
    old_val >>= shift;
    old_val  &= mask;
    /* if need to sign extend */
    if ( tchar == 'I' && ( old_val.ival & signb ) != 0 ) {
      int72_t tmp;
      msk_val  = mask;
      old_val |= msk_val.notval( tmp );
    }
    if ( bf[ i ].op == OP_INCRBY ) {
      int64_t incr    = bf[ i ].val,
              new_val = incr + old_val.ival;
      if ( tchar == 'I' ) {
        if ( bf[ i ].overflow == OV_WRAP ) { /* wrap the result */
          if ( ( new_val & signb ) == 0 )
            new_val &= mask;
          else
            new_val |= ~mask;
        }
        else {
          const int64_t max_val  = mask / 2,          /* max positive int */
                        min_val  = -( mask / 2 + 1 ); /* min negative int */
          const uint64_t max_diff = (uint64_t) ( max_val - old_val.ival ),
                         min_diff = (uint64_t) ( old_val.ival - min_val );
          /* check that incr is within range */
          if ( incr > 0 && (uint64_t) incr > max_diff ) {
            new_val = max_val;
            fail = ( bf[ i ].overflow == OV_FAIL );
          }
          else if ( incr < 0 && (uint64_t) -incr > min_diff ) {
            new_val = min_val;
            fail = ( bf[ i ].overflow == OV_FAIL );
          }
        }
      }
      else {
        if ( bf[ i ].overflow == OV_WRAP ) /* wrap the result */
          new_val &= mask;
        else { /* no 64 bit uint, the extra bit allows overflow comparison */
          if ( incr > 0 && (uint64_t) new_val > mask ) {
            new_val = mask;
            fail = ( bf[ i ].overflow == OV_FAIL );
          }
          else if ( incr < 0 && new_val < 0 ) {
            new_val = 0;
            fail = ( bf[ i ].overflow == OV_FAIL );
          }
        }
      }
      old_val.ival = new_val;
      bf[ i ].val  = new_val;
    }
    /* replace current value with new value */
    if ( bf[ i ].op != OP_GET && ! fail ) {
      int72_t tmp;
      msk_val   = mask;
      msk_val <<= shift;
      upd_val  &= msk_val.notval( tmp ); /* upd_val = upd_val & ~( mask << shift ) */
      msk_val   = (uint64_t) bf[ i ].val & mask;
      msk_val <<= shift;
      upd_val  |= msk_val;/* upd_val = upd_val | ( ( ival & mask ) << shift ) */
      for ( j = 0; start < off; ) {
        ((uint8_t *) data)[ start++ ] = upd_val.b[ j++ ];
      }
    }
    /* send the int val */
    if ( ! fail ) {
      str[ sz ] = ':';
      sz += 1 + ( tchar == 'I' ?
                  int_to_str( old_val.ival, &str[ sz + 1 ] ) :
                  uint_to_str( old_val.ival, &str[ sz + 1 ] ) );
    }
    /* incrby "overflow fail" failed, XXX this returns nil which is what redis
     * does but documentation says it should return null */
    else { /* $-1 */
      str[ sz ] = '$'; str[ sz + 1 ] = '-'; str[ sz + 2 ] = '1';
      sz += 3;
    }
    sz = crlf( str, sz );
  }
  if ( ctx.is_read_only() ) {
    ctx.kstatus = this->kctx.validate_value();
    if ( ctx.kstatus != KEY_OK )
      return ERR_KV_STATUS;
  }
  this->strm.sz += sz;
  return EXEC_OK;
}

typedef void (*f64_t)( uint64_t &, uint64_t );
typedef void (*f32_t)( uint32_t &, uint32_t );
typedef void (*f16_t)( uint16_t &, uint16_t );
typedef void (*f8_t)( uint8_t &, uint8_t );

static inline void
bitop( f64_t op64, f32_t op32, f16_t op16, f8_t op8, void *x, void *y,
       size_t sz )
{
  size_t i;
  for ( i = 0; i + 8 <= sz; i += 8 ) {
    op64( *(uint64_t *) &((uint8_t *) x)[ i ],
          *(uint64_t *) &((uint8_t *) y)[ i ] );
  }
  if ( i + 4 <= sz ) {
    op32( *(uint32_t *) &((uint8_t *) x)[ i ],
          *(uint32_t *) &((uint8_t *) y)[ i ] );
    i += 4;
  }
  if ( i + 2 <= sz ) {
    op16( *(uint16_t *) &((uint8_t *) x)[ i ],
          *(uint16_t *) &((uint8_t *) y)[ i ] );
    i += 2;
  }
  if ( i + 1 <= sz )
    op8( ((uint8_t *) x)[ i ], ((uint8_t *) y)[ i ] );
}

template <class Int> static inline void andt( Int &x, Int y ) { x &= y; }
template <class Int> static inline void ort( Int &x, Int y )  { x |= y; }
template <class Int> static inline void xort( Int &x, Int y ) { x ^= y; }
template <class Int> static inline void nott( Int &x, Int y ) { x = ~y; }

static void and_bits( void *x,  void *y,  size_t sz ) {
  bitop(andt<uint64_t>,andt<uint32_t>,andt<uint16_t>,andt<uint8_t>, x,y,sz );
}
static void or_bits( void *x,  void *y,  size_t sz ) {
  bitop(ort<uint64_t>,ort<uint32_t>,ort<uint16_t>,ort<uint8_t>, x,y,sz );
}
static void xor_bits( void *x,  void *y,  size_t sz ) {
  bitop(xort<uint64_t>,xort<uint32_t>,xort<uint16_t>,xort<uint8_t>, x,y,sz );
}
static void not_bits( void *x,  size_t sz ) {
  bitop(nott<uint64_t>,nott<uint32_t>,nott<uint16_t>,nott<uint8_t>, x,x,sz );
}

ExecStatus
RedisExec::exec_bitop( EvKeyCtx &ctx ) noexcept
{
  enum { BIT_AND_OP = 0, BIT_OR_OP = 1, BIT_XOR_OP = 3, BIT_NOT_OP = 4 } op;
  void * data,
       * part_data;
  size_t part_size, sz, extra_sz;

  /* BITOP OP dest src [src2 src3 ..] */
  if ( ctx.argn == 2 /* dest */ ) {
    if ( ctx.dep == 0 ) /* handle dest key after all the src keys */
      return EXEC_DEPENDS;
    ctx.ival = 0;
    for ( size_t i = 1; i < this->key_cnt; i++ )
      ctx.ival = max<size_t>( this->keys[ i ]->part->size, ctx.ival );

    switch ( this->msg.match_arg( 1, MARG( "and" ),
                                     MARG( "or" ),
                                     MARG( "xor" ),
                                     MARG( "not" ), NULL ) ) {
      default: return ERR_BAD_ARGS;
      case 1: op = BIT_AND_OP; break;
      case 2: op = BIT_OR_OP;  break;
      case 3: op = BIT_XOR_OP; break;
      case 4:
        op = BIT_NOT_OP;
        if ( this->argc > 4 )
          return ERR_BAD_ARGS;
        break;
    }

    switch ( this->get_key_write( ctx, MD_STRING ) ) { /* write access */
      case KEY_OK:
      case KEY_IS_NEW:
        ctx.kstatus = this->kctx.resize( &data, ctx.ival );
        if ( ctx.kstatus == KEY_OK ) {
          part_data = this->keys[ 1 ]->part->data( 0 ); /* 1st src key */
          part_size = this->keys[ 1 ]->part->size;
          extra_sz  = (size_t) ctx.ival - part_size;
          ::memcpy( data, part_data, part_size );
          if ( extra_sz > 0 )
            ::memset( &((uint8_t *) data)[ part_size ], 0, extra_sz );
          if ( op == BIT_NOT_OP )
            not_bits( data, ctx.ival );

          for ( uint32_t k = 2; k < this->key_cnt; k++ ) { /* other src keys */
            part_data = this->keys[ k ]->part->data( 0 );
            part_size = this->keys[ k ]->part->size;
            sz        = min<size_t>( part_size, ctx.ival );
            extra_sz  = (size_t) ctx.ival - part_size;
            switch ( op ) {
              case BIT_AND_OP:
                and_bits( data, part_data, sz );
                if ( extra_sz > 0 )
                  ::memset( &((uint8_t *) data)[ sz ], 0, extra_sz );
                break;
              case BIT_OR_OP:
                or_bits( data, part_data, sz );
                break;
              case BIT_XOR_OP:
                xor_bits( data, part_data, sz );
                if ( extra_sz > 0 )
                  not_bits( &((uint8_t *) data)[ sz ], extra_sz );
                break;
              case BIT_NOT_OP: break;
            }
          }
          ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
          return EXEC_SEND_INT;
        }
        /* FALLTHRU */
      default:           return ERR_KV_STATUS;
      case KEY_NO_VALUE: return ERR_BAD_TYPE;
    }
  }
  data = NULL;
  sz   = 0;
  switch ( this->get_key_read( ctx, MD_STRING ) ) { /* read access */
    case KEY_OK:
      if ( (ctx.kstatus = this->kctx.value( &data, sz )) == KEY_OK ) {
    case KEY_NOT_FOUND:
        this->save_data( ctx, data, sz );
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK )
          return EXEC_OK;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_bitpos( EvKeyCtx &ctx ) noexcept
{
  int64_t  bit,
           start_off = 0,
           end_off   = -1;
  void   * data;
  size_t   size;

  if ( ! this->msg.get_arg( 2, bit ) )
    return ERR_BAD_ARGS;

  if ( (uint64_t) bit > 1 )
    return ERR_BAD_ARGS;

  if ( this->argc > 3 && ! this->msg.get_arg( 3, start_off ) )
    return ERR_BAD_ARGS;
  if ( this->argc > 4 && ! this->msg.get_arg( 4, end_off ) )
    return ERR_BAD_ARGS;
  if ( start_off < 0 )
    return ERR_BAD_ARGS;

  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        const uint8_t * start = (const uint8_t *) data,
                      * end   = &start[ size ];
        if ( end_off > start_off ) {
          if ( (uint64_t) end_off < size )
            end = &start[ end_off ];
        }
        if ( start_off > 0 )
          start = &start[ start_off ];
        ctx.ival = -1; /* result if no bit found */
        int z; /* find the bit flip */
        for ( const uint8_t *p = start; p < end; p = &p[ z ] ) {
          uint64_t x;
          if ( end - p >= 8 ) {
            x = *(const uint64_t *) (const void *) p;
            z = 8;
          }
          else if ( end - p >= 4 ) {
            x = *(const uint32_t *) (const void *) p;
            z = 4;
          }
          else if ( end - p >= 2 ) {
            x = *(const uint16_t *) (const void *) p;
            z = 2;
          }
          else {
            x = *p;
            z = 1;
          }
          if ( bit == 0 ) /* bit is 0 or 1 */
            x = ~x;
          if ( x != 0 ) {
            ctx.ival  = ( p - (const uint8_t *) data ) * 8;
            ctx.ival += __builtin_ctzl( x ); /* 0 -> 63 */
            if ( (uint64_t) ctx.ival >= size * 8 )
              ctx.ival = -1; /* if no bit exists within size bytes */
            break;
          }
        }
        if ( (ctx.kstatus = this->kctx.validate_value()) == KEY_OK )
          return EXEC_SEND_INT;
      }
      /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_decr( EvKeyCtx &ctx ) noexcept
{
  return this->do_add( ctx, -1 );
}

ExecStatus
RedisExec::exec_decrby( EvKeyCtx &ctx ) noexcept
{
  int64_t decr;
  if ( ! this->msg.get_arg( 2, decr ) )
    return ERR_BAD_ARGS;
  return this->do_add( ctx, -decr );
}

ExecStatus
RedisExec::exec_get( EvKeyCtx &ctx ) noexcept
{
  void * data;
  size_t size;
  /* GET key */
  switch ( this->get_key_read( ctx, MD_STRING ) )
    case KEY_OK: {
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        size_t sz = this->send_string( data, size );
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK ) {
          this->strm.sz += sz;
          return EXEC_OK;
        }
      }
      /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_getbit( EvKeyCtx &ctx ) noexcept
{
  int64_t off;
  void  * data;
  size_t  size;

  if ( ! this->msg.get_arg( 2, off ) ) /* GETBIT key bit-offset */
    return ERR_BAD_ARGS;

  if ( off < 0 )
    return EXEC_SEND_ZERO;

  uint64_t byte_off = off / 8;
  uint8_t  mask     = ( 1U << ( off % 8 ) );

  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        bool is_one = false;
        if ( byte_off < size &&
             ( ( (uint8_t *) data)[ byte_off ] & mask ) != 0 )
          is_one = true;
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK )
          return is_one ? EXEC_SEND_ONE : EXEC_SEND_ZERO;
      }
      /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_getrange( EvKeyCtx &ctx ) noexcept
{
  int64_t    start  = 0,
             end    = -1;
  size_t     sz     = 0,
             len    = 0;
  int64_t    start_off,
             end_off;
  ExecStatus status = EXEC_OK;
  void     * data;
  size_t     size;

  /* GETRANGE KEY [start end] */
  if ( this->argc > 2 && ! this->msg.get_arg( 2, start ) )
    return ERR_BAD_ARGS;
  if ( this->argc > 3 && ! this->msg.get_arg( 3, end ) )
    return ERR_BAD_ARGS;

  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        if ( size == 0 )
          status = EXEC_SEND_ZERO_STRING;
        else {
          start_off = ( start < 0 ) ? (int64_t) size + start : start;
          end_off   = ( end   < 0 ) ? (int64_t) size + end   : end;
          /* if endpoints outside string range */
          if ( start_off > end_off ||
               ( start_off < 0 && end_off < 0 ) ||
               ( start_off >= (int64_t) size && end_off >= (int64_t) size ) )
            status = EXEC_SEND_ZERO_STRING;
          else {
            /* clip segment [start, end] to [0, size-1] */
            start_off = min<int64_t>( max<int64_t>( start_off, 0 ), size - 1 );
            end_off   = max<int64_t>( min<int64_t>( end_off, size - 1 ), 0 );
            if ( end_off >= start_off )
              len = ( end_off + 1 ) - start_off;

            sz = this->send_string( &((uint8_t *) data)[ start_off ], len );
          }
        }
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK ) {
          if ( status == EXEC_OK )
            this->strm.sz += sz;
          return status;
        }
      }
      /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_ZERO_STRING;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_getset( EvKeyCtx &ctx ) noexcept
{
  const char * value;
  size_t       valuelen,
               sz = 0;
  void       * data;
  size_t       size;

  if ( ! this->msg.get_arg( 2, value, valuelen ) ) /* GETSET key value */
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) { /* write access */
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      sz = this->send_string( data, size );
      /* FALLTHRU */
    case KEY_IS_NEW:
      this->kctx.clear_stamps( true, false );
      ctx.kstatus = this->kctx.resize( &data, valuelen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, value, valuelen );
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
        if ( ctx.is_new() )
          return EXEC_SEND_NIL;
        this->strm.sz += sz;
        return EXEC_OK;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_incr( EvKeyCtx &ctx ) noexcept
{
  return this->do_add( ctx, 1 ); /* INCR key */
}

ExecStatus
RedisExec::exec_incrby( EvKeyCtx &ctx ) noexcept
{
  int64_t incr;
  if ( ! this->msg.get_arg( 2, incr ) ) /* INCRBY key incr */
    return ERR_BAD_ARGS;
  return this->do_add( ctx, incr );
}

ExecStatus
RedisExec::do_add( EvKeyCtx &ctx,  int64_t incr ) noexcept /* incr/decr value */
{
  void * data;
  char * str;
  size_t size,
         sz;

  ctx.ival = 0;
  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      if ( size > 0 ) {
        this->mstatus = (RedisMsgStatus)
                        string_to_int( (char *) data, size, ctx.ival );
        /*if ( this->mstatus != REDIS_MSG_OK )
          return ERR_MSG_STATUS;*/
      }
      /* FALLTHRU */
    case KEY_IS_NEW:
      ctx.ival += incr;
      str = this->strm.alloc( 32 );
      str[ 0 ] = ':';
      sz = 1 + int_to_str( ctx.ival, &str[ 1 ] );
      sz = crlf( str, sz );
      ctx.kstatus = this->kctx.resize( &data, sz - 3 );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, &str[ 1 ], sz - 3 );
        this->strm.sz += sz;
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
        return EXEC_OK;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_incrbyfloat( EvKeyCtx &ctx ) noexcept
{
  char         fpdata[ 64 ];
  Decimal128   fp;
  const char * fval;
  size_t       fvallen;
  void       * data;
  char       * str;
  size_t       size;
  size_t       sz;

  if ( ! this->msg.get_arg( 2, fval, fvallen ) ) /* INCRBYFLOAT key value */
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      if ( size > 0 ) {
        fp = Decimal128::parse_len( (const char *) data, size );
      }
      else {
        /* FALLTHRU */
    case KEY_IS_NEW:
        fp.zero();
      }
      fp += Decimal128::parse_len( fval, fvallen );
      fvallen = fp.to_string( fpdata );
      sz = 32 + fvallen * 2;
      str = this->strm.alloc( sz );
      str[ 0 ] = '$';
      sz = 1 + int_to_str( fvallen, &str[ 1 ] );
      sz = crlf( str, sz );
      ::memcpy( &str[ sz ], fpdata, fvallen );
      sz = crlf( str, sz + fvallen );
      ctx.kstatus = this->kctx.resize( &data, fvallen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, fpdata, fvallen );
        this->strm.sz += sz;
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
        return EXEC_OK;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_mget( EvKeyCtx &ctx ) noexcept
{
  void   * data;
  uint64_t size;
  /* MGET key [key2 key3] */
  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        if ( ! this->save_string_result( ctx, data, size ) )
          return ERR_ALLOC_FAIL;
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK )
          return EXEC_OK;
      }
      /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND:
    case KEY_NO_VALUE: {
      ctx.part = NULL;
      return EXEC_OK; /*return EXEC_SEND_NIL; */
    }
  }
}

ExecStatus
RedisExec::exec_mset( EvKeyCtx &ctx ) noexcept
{
  return this->do_set_value( ctx, ctx.argn+1, 0 );
}

ExecStatus
RedisExec::exec_msetnx( EvKeyCtx &ctx ) noexcept
{
  if ( ctx.dep == 0 ) {
    /* if a key already exists, send zero and make no changes */
    if ( this->exec_key_fetch( ctx, true ) == KEY_OK )
      return EXEC_ABORT_SEND_ZERO;
    /* test all the keys */
    if ( this->key_done + 1 < this->key_cnt )
      return EXEC_DEPENDS;
  }
  /* set the value second time around */
  return this->do_set_value( ctx, ctx.argn+1, 0 );
}

ExecStatus
RedisExec::exec_psetex( EvKeyCtx &ctx ) noexcept
{
  int64_t  ival;
  uint64_t ns;

  if ( ! this->msg.get_arg( 2, ival ) )
    return ERR_BAD_ARGS;
  ns = (uint64_t) ival * 1000 * 1000;
  /* PSET key ms value */
  return this->do_set_value_expire( ctx, 3, ns, HAS_EXPIRE_NS );
}

ExecStatus
RedisExec::exec_set( EvKeyCtx &ctx ) noexcept
{
  const char * op;
  size_t       oplen;
  int64_t      ival;
  uint64_t     ns    = 0;
  int          flags = 0;

  if ( this->argc > 3 ) {
    for ( int i = 3; i < (int) this->argc; ) {
      if ( ! this->msg.get_arg( i, op, oplen ) ||
           ( oplen != 2 || toupper( op[ 1 ] ) != 'X' ) )
        return ERR_BAD_ARGS;
      switch ( toupper( op[ 0 ] ) ) {
        case 'E':                     /* SET key value [EX secs] */
          if ( ! this->msg.get_arg( i + 1, ival ) )
            return ERR_BAD_ARGS;
          ns = (uint64_t) ival * 1000 * 1000 * 1000;
          flags |= HAS_EXPIRE_NS;
          i += 2;
          break;
        case 'P':                     /* SET key value [PX ms] */
          if ( ! this->msg.get_arg( i + 1, ival ) )
            return ERR_BAD_ARGS;
          ns = (uint64_t) ival * 1000 * 1000;
          flags |= HAS_EXPIRE_NS;
          i += 2;
          break;
        case 'N':                     /* SET key value [NX] */
          flags |= K_MUST_NOT_EXIST;
          i += 1;
          break;
        case 'X':                     /* SET key value [XX] */
          flags |= K_MUST_EXIST;
          i += 1;
          break;
        default:
          return ERR_BAD_ARGS;
      }
    }
  }
  if ( ( flags & HAS_EXPIRE_NS ) != 0 )
    return this->do_set_value_expire( ctx, 2, ns, flags );
  return this->do_set_value( ctx, 2, flags );
}

ExecStatus
RedisExec::do_set_value_expire( EvKeyCtx &ctx,  int n,  uint64_t ns,
                                int flags ) noexcept
{
  const char * value;
  size_t       valuelen;
  void       * data;

  if ( ! this->msg.get_arg( n, value, valuelen ) ) /* SET value w/clear EX */
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_NO_VALUE: /* overwrite key */
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( ( flags & ( K_MUST_NOT_EXIST | K_MUST_EXIST ) ) != 0 ) {
        if ( ctx.is_new() && ( flags & K_MUST_EXIST ) != 0 )
          return EXEC_SEND_NIL;
        if ( ! ctx.is_new() && ( flags & K_MUST_NOT_EXIST ) != 0 )
          return EXEC_SEND_NIL;
      }
      ctx.kstatus = this->kctx.resize( &data, valuelen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, value, valuelen );
        if ( ns != 0 && ns < TEN_YEARS_NS )
          ns += this->kctx.ht.hdr.current_stamp;
        this->kctx.update_stamps( ns, 0 );
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
        return EXEC_SEND_OK;
      }
      /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::do_set_value( EvKeyCtx &ctx,  int n,  int flags ) noexcept
{
  const char * value;
  size_t       valuelen;
  void       * data;

  if ( ! this->msg.get_arg( n, value, valuelen ) ) /* SET value w/set EX */
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_NO_VALUE: /* overwrite key */
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = MD_STRING;
      /* FALLTHRU */
    case KEY_OK:
    case KEY_IS_NEW:
      if ( ( flags & ( K_MUST_NOT_EXIST | K_MUST_EXIST ) ) != 0 ) {
        if ( ctx.is_new() && ( flags & K_MUST_EXIST ) != 0 )
          return EXEC_SEND_NIL;
        if ( ! ctx.is_new() && ( flags & K_MUST_NOT_EXIST ) != 0 )
          return EXEC_SEND_NIL;
      }
      this->kctx.clear_stamps( true, false );
      ctx.kstatus = this->kctx.resize( &data, valuelen );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, value, valuelen );
        ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
        if ( this->cmd == MSETNX_CMD )
          return EXEC_SEND_ONE;
        return EXEC_SEND_OK;
      }
      /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }
}

ExecStatus
RedisExec::exec_setbit( EvKeyCtx &ctx ) noexcept
{
  int64_t  off,
           bitval;
  void   * data;
  uint64_t data_sz = 0,
           byte_off,
           new_sz;
  uint8_t  bit_mask;

  if ( ! this->msg.get_arg( 2, off ) || off < 0 ) /* SETBIT key off value */
    return ERR_BAD_ARGS;
  if ( ! this->msg.get_arg( 3, bitval ) || (uint64_t) bitval > 1 )
    return ERR_BAD_ARGS;

  byte_off = (uint64_t) off / 8;
  bit_mask = (uint8_t) ( 1U << ( (uint64_t) off % 8 ) );
  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.get_size( data_sz );
      if ( ctx.kstatus == KEY_OK ) {
    case KEY_IS_NEW:
        new_sz = max<uint64_t>( data_sz, byte_off + 1 );
        ctx.kstatus = this->kctx.resize( &data, new_sz, true );
        if ( ctx.kstatus == KEY_OK && new_sz > data_sz )
          ::memset( &((uint8_t *) data)[ data_sz ], 0, new_sz - data_sz );
        if ( ctx.kstatus == KEY_OK ) {
          uint8_t &v = ((uint8_t *) data)[ byte_off ];
          ctx.ival = ( v & bit_mask ) ? 1 : 0;
          if ( bitval == 0 )
            v &= ~bit_mask;
          else
            v |= bit_mask;
          return EXEC_SEND_INT;
        }
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_setex( EvKeyCtx &ctx ) noexcept
{
  int64_t  ival;
  uint64_t ns;

  if ( ! this->msg.get_arg( 2, ival ) ) /* SETEX key secs value */
    return ERR_BAD_ARGS;
  ns = (uint64_t) ival * 1000 * 1000 * 1000;
  return this->do_set_value_expire( ctx, 3, ns, HAS_EXPIRE_NS );
}

ExecStatus
RedisExec::exec_setnx( EvKeyCtx &ctx ) noexcept
{
  return this->do_set_value( ctx, 2, K_MUST_NOT_EXIST ); /* SETNX key value */
}

ExecStatus
RedisExec::exec_setrange( EvKeyCtx &ctx ) noexcept
{
  void       * data    = NULL;
  int64_t      off;
  uint64_t     data_sz = 0,
               new_sz;
  const char * value;
  size_t       valuelen;

  if ( ! this->msg.get_arg( 2, off ) || off < 0 ) /* SETRANGE key off value */
    return ERR_BAD_ARGS;
  if ( ! this->msg.get_arg( 3, value, valuelen ) )
    return ERR_BAD_ARGS;

  switch ( this->get_key_write( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.get_size( data_sz );
      if ( ctx.kstatus == KEY_OK ) {
    case KEY_IS_NEW:
        new_sz = max<uint64_t>( data_sz, off + valuelen );
        ctx.ival = (int64_t) new_sz;
        ctx.kstatus = this->kctx.resize( &data, new_sz, true );
        if ( ctx.kstatus == KEY_OK ) {
          if ( (uint64_t) off > data_sz ) /* pad with zeros */
            ::memset( &((uint8_t *) data)[ data_sz ], 0, off - data_sz );
          ::memcpy( &((uint8_t *) data)[ off ], value, valuelen );
          ctx.flags |= EKF_KEYSPACE_EVENT | EKF_KEYSPACE_STRING;
          return EXEC_SEND_INT;
        }
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}

ExecStatus
RedisExec::exec_strlen( EvKeyCtx &ctx ) noexcept
{
  uint64_t data_sz = 0;
  /* STRLEN key */
  switch ( this->get_key_read( ctx, MD_STRING ) ) {
    case KEY_OK:
      ctx.kstatus = this->kctx.get_size( data_sz );
      if ( ctx.kstatus == KEY_OK ) {
    case KEY_NOT_FOUND:
        ctx.ival = data_sz;
        return EXEC_SEND_INT;
      }
      /* FALLTHRU */
    default:           return ERR_KV_STATUS;
    case KEY_NO_VALUE: return ERR_BAD_TYPE;
  }
}
