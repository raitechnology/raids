#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raimd/md_types.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raids/pattern_cvt.h>

using namespace rai;
using namespace ds;
using namespace kv;
using namespace md;

ExecStatus
RedisExec::exec_del( EvKeyCtx &ctx )
{
  /* DEL key1 [key2 ...] */
  if ( this->exec_key_fetch( ctx, true ) == KEY_OK && /* test exists */
       this->exec_key_fetch( ctx ) == KEY_OK ) {
    this->kctx.tombstone();
    ctx.ival = 1;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_dump( EvKeyCtx &ctx )
{
  /* DUMP key (serialize key) */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK: {
      uint64_t off,
               size;
      char   * buf;
      void   * data;
      ctx.kstatus = this->kctx.value( &data, size );
      if ( ctx.kstatus == KEY_OK ) {
        size = this->kctx.hash_entry_size;
        if ( this->kctx.entry->test( FL_SEGMENT_VALUE ) )
          size += this->kctx.msg->size;
        buf = (char *) this->strm.alloc_temp( size + 34 );
        if ( buf == NULL )
          return ERR_ALLOC_FAIL;
        ::memcpy( &buf[ 32 ], this->kctx.entry, this->kctx.hash_entry_size );
        if ( this->kctx.entry->test( FL_SEGMENT_VALUE ) )
          ::memcpy( &buf[ 32 + this->kctx.hash_entry_size ], this->kctx.msg,
                    size - this->kctx.hash_entry_size );
        ctx.kstatus = this->kctx.validate_value();
        if ( ctx.kstatus == KEY_OK ) {
          buf[ 0 ] = '$';
          off = 1 + RedisMsg::int_to_str( size, &buf[ 1 ] );
          off = crlf( buf, off );
          ::memmove( &buf[ 32 - off ], buf, off );
          off = crlf( buf, 32 + size );
          crlf( buf, 32 + size );
          this->strm.append_iov( &buf[ 32 - off ], size + off + 2 );
          return EXEC_OK;
        }
      }
    }
    /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
  }
}

ExecStatus
RedisExec::exec_exists( EvKeyCtx &ctx )
{
  /* EXISTS key1 [key2 ...] */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK:        ctx.ival = 1; break;
    case KEY_NOT_FOUND: ctx.ival = 0; break;
    default:            return ERR_KV_STATUS;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_expire( EvKeyCtx &ctx )
{
  /* EXPIRE key secs */
  return this->do_pexpire( ctx, 1000 * 1000 * 1000 );
}

ExecStatus
RedisExec::exec_expireat( EvKeyCtx &ctx )
{
  /* EXPIREAT key stamp */
  return this->do_pexpireat( ctx, 1000 * 1000 * 1000 );
}

/* XXX add option to synchronize scanning to prevent multiple copies of same k*/
ExecStatus
RedisExec::exec_keys( void )
{
  ScanArgs     sa;
  const char * pattern;
  size_t       patlen;
  ExecStatus   status;
  /* KEYS pattern */
  if ( ! this->msg.get_arg( 1, pattern, patlen ) )
    return ERR_BAD_ARGS;
  /* if not matching everything */
  if ( patlen > 1 || pattern[ 0 ] != '*' ) {
    char       buf[ 1024 ];
    size_t     erroff;
    int        error;
    PatternCvt cvt( buf, sizeof( buf ) );

    if ( cvt.convert_glob( pattern, patlen ) != 0 )
      return ERR_BAD_ARGS;

    sa.re = pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
    if ( sa.re == NULL ) {
      return ERR_BAD_ARGS;
    }
    sa.md = pcre2_match_data_create_from_pattern( sa.re, NULL );
    if ( sa.md == NULL ) {
      pcre2_code_free( sa.re );
      sa.re = NULL;
      return ERR_BAD_ARGS;
    }
  }
  sa.maxcnt = -1;
  status = this->scan_keys( sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::exec_migrate( EvKeyCtx &/*ctx*/ )
{
  /* MIGRATE host port key */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_move( EvKeyCtx &/*ctx*/ )
{
  /* MOVE key db# */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_object( EvKeyCtx &ctx )
{
  /* OBJECT key [refcount|encoding|idletime|freq|help] */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK:        break;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    default:            return ERR_KV_STATUS;
  }
  switch ( this->msg.match_arg( 1, "refcount", 8,
                                   "encoding", 8,
                                   "idletime", 8,
                                   "freq",     4,
                                   "help",     4, NULL ) ) {
    case 1: /* refcount */
      ctx.ival = 1;
      return EXEC_SEND_INT;
    case 2: { /* encoding */
      const char *str = ctx.get_type_str();
      this->strm.sz += this->send_string( (void *) str, ::strlen( str ) );
      return EXEC_OK;
    }
    case 3: { /* idletime */
      uint64_t exp_ns, upd_ns;
      ctx.kstatus = this->kctx.get_stamps( exp_ns, upd_ns );
      if ( ctx.kstatus != KEY_OK )
        return ERR_KV_STATUS;
      if ( upd_ns != 0 )
        ctx.ival = ( this->kctx.ht.hdr.current_stamp - upd_ns ) /
                   ( 1000 * 1000 * 1000 );
      else
        ctx.ival = 0;
      return EXEC_SEND_INT;
    }
    case 4: /* freq */
      if ( this->kctx.entry->test( FL_SEGMENT_VALUE ) ) {
        this->kctx.entry->get_value_geom( this->kctx.hash_entry_size,
                                          this->kctx.geom,
                                          this->kctx.seg_align_shift );
        ctx.ival = this->kctx.geom.serial - ( kctx.key & ValueCtr::SERIAL_MASK);
      }
      else {
        ctx.ival = kctx.serial - ( kctx.key & ValueCtr::SERIAL_MASK );
      }
      return EXEC_SEND_INT;
    default:
    case 5: /* help */
      return ERR_BAD_ARGS;
  }
}

ExecStatus
RedisExec::exec_persist( EvKeyCtx &ctx )
{
  /* PERSIST key */
  if ( this->exec_key_fetch( ctx ) == KEY_OK ) {
    this->kctx.clear_stamps( true, false );
    ctx.ival = 1;
  }
  else {
    ctx.ival = 0;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_pexpire( EvKeyCtx &ctx )
{
  /* PEXPIRE key ms */
  return this->do_pexpire( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pexpire( EvKeyCtx &ctx,  uint64_t units )
{
  int64_t  ival;
  uint64_t exp;
  if ( ! this->msg.get_arg( 2, ival ) ) /* SETEX key secs value */
    return ERR_BAD_ARGS;
  exp = (uint64_t) ival * units;
  if ( exp < this->kctx.ht.hdr.current_stamp )
    exp += this->kctx.ht.hdr.current_stamp;
  if ( this->exec_key_fetch( ctx ) == KEY_OK ) {
    this->kctx.update_stamps( exp, 0 );
    ctx.ival = 1;
  }
  else {
    ctx.ival = 0;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_pexpireat( EvKeyCtx &ctx )
{
  /* PEXPIREAT key stamp */
  return this->do_pexpireat( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pexpireat( EvKeyCtx &ctx,  uint64_t units )
{
  int64_t  ival;
  uint64_t exp;
  if ( ! this->msg.get_arg( 2, ival ) ) /* SETEX key secs value */
    return ERR_BAD_ARGS;
  exp = (uint64_t) ival * units;
  if ( this->exec_key_fetch( ctx ) == KEY_OK ) {
    this->kctx.update_stamps( exp, 0 );
    ctx.ival = 1;
  }
  else {
    ctx.ival = 0;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_pttl( EvKeyCtx &ctx )
{
  /* PTTL key */
  return this->do_pttl( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pttl( EvKeyCtx &ctx,  int64_t units )
{
  if ( this->exec_key_fetch( ctx ) == KEY_OK ) {
    uint64_t exp = 0, upd;
    this->kctx.get_stamps( exp, upd );
    if ( exp > 0 )
      exp -= this->kctx.ht.hdr.current_stamp;
    ctx.ival = (int64_t) exp / units;
  }
  else {
    ctx.ival = -1;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_randomkey( void )
{
  uint64_t pos     = this->kctx.thr_ctx.rng.next();
  uint64_t ht_size = this->kctx.ht_size;

  pos = this->kctx.ht.hdr.ht_mod( pos ); /* RANDOMKEY */
  for ( uint64_t cnt = 0; cnt < ht_size; cnt++ ) {
    KeyStatus status = this->kctx.fetch( &this->wrk, pos, 0, true );
    if ( status == KEY_OK ) {
      KeyFragment *kp;
      status = this->kctx.get_key( kp );
      if ( status == KEY_OK ) {
        uint16_t keylen = kp->keylen;
        if ( keylen > 0 && kp->u.buf[ keylen - 1 ] == '\0' )
          keylen--;
        this->strm.sz += this->send_string( kp->u.buf, keylen );
        return EXEC_OK;
      }
    }
    if ( ++pos == ht_size )
      pos = 0;
  }
  return EXEC_SEND_NIL;
}

/* XXX do atomic rename */
ExecStatus
RedisExec::exec_rename( EvKeyCtx &ctx )
{
  void * data;
  size_t sz;
  /* RENAME key newkey */
  if ( ctx.argn == 2 ) { /* newkey dest */
    if ( this->keys[ 0 ]->part == NULL ) /* read key before writing newkey */
      return EXEC_DEPENDS;

    sz = this->keys[ 0 ]->part->size; /* key saved here */
    if ( sz == 0 )
      return ERR_KEY_DOESNT_EXIST;

    switch ( this->exec_key_fetch( ctx ) ) { /* write access */
      case KEY_OK:
      case KEY_IS_NEW:
        ctx.kstatus = this->kctx.resize( &data, sz );
        if ( ctx.kstatus == KEY_OK ) {
          ::memcpy( data, this->keys[ 0 ]->part->data( 0 ), sz ); /* key data */
          this->kctx.set_type( this->keys[ 0 ]->type );
          this->kctx.set_val( 0 );
          /* inherits expire time? */
          if ( this->cmd == RENAME_CMD )
            return EXEC_SEND_OK;
          return EXEC_SEND_INT;
        }
        /* fall through */
      default: return ERR_KV_STATUS;
    }
  }

  if ( ctx.dep == 0 ) { /* fetch the value first */
    data = NULL;
    sz   = 0;
    switch ( this->exec_key_fetch( ctx, true ) ) { /* read access */
      case KEY_OK:
        if ( (ctx.kstatus = this->kctx.value( &data, sz )) == KEY_OK ) {
      case KEY_NOT_FOUND:
          this->save_data( ctx, data, sz, 0 );
          ctx.kstatus = this->kctx.validate_value();
          if ( ctx.kstatus == KEY_OK )
            return EXEC_DEPENDS; /* redo again after saving the value */
        }
        /* fall through */
      default: return ERR_KV_STATUS;
    }
  }
  else { /* delete the old data, it has been saved above */
    switch ( this->exec_key_fetch( ctx ) ) { /* write access for del */
      case KEY_OK:
      case KEY_IS_NEW:
        this->kctx.tombstone();
        if ( this->cmd == RENAME_CMD )
          return EXEC_SEND_OK;
        /* renamenx */
        ctx.ival = 1;
        return EXEC_SEND_INT;

      default: return ERR_KV_STATUS;
    }
  }
}

ExecStatus
RedisExec::exec_renamenx( EvKeyCtx &ctx )
{
  /* RENAMENX key newkey */
  if ( ctx.argn == 2 ) { /* newkey dest */
    if ( ctx.dep == 0 ) { /* test if key exists */
      switch ( this->exec_key_fetch( ctx, true ) ) {
        case KEY_NOT_FOUND:
          return this->exec_rename( ctx );
        case KEY_OK:
          return EXEC_ABORT_SEND_ZERO;
        default: return ERR_KV_STATUS;
      }
    }
  }
  return this->exec_rename( ctx );
}

ExecStatus
RedisExec::exec_restore( EvKeyCtx &/*ctx*/ )
{
  /* RESTORE key ttl value */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sort( EvKeyCtx &/*ctx*/ )
{
  /* SORT key [BY pat] [LIMIT off cnt] [GET pat] [ASC|DESC] 
   *          [ALPHA] [STORE dest] */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_touch( EvKeyCtx &ctx )
{
  uint64_t upd = this->kctx.ht.hdr.current_stamp;
  /* TOUCH key [key2 ...] */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK: this->kctx.update_stamps( 0, upd ); ctx.ival = 1; break;
    case KEY_IS_NEW: ctx.ival = 0; break;
    default: return ERR_KV_STATUS;
  }
  return EXEC_SEND_INT;
}

ExecStatus
RedisExec::exec_ttl( EvKeyCtx &ctx )
{
  /* TTL key */
  return this->do_pttl( ctx, 1000 * 1000 * 1000 );
}

ExecStatus
RedisExec::exec_type( EvKeyCtx &ctx )
{
  const char *str;
  size_t len;
  /* TYPE key */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK:        str = ctx.get_type_str(); len = ::strlen( str ); break;
    case KEY_NOT_FOUND: str = "none"; len = 4; break;
    default:            return ERR_KV_STATUS;
  }
  this->strm.sz += this->send_string( (void *) str, len );
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_unlink( EvKeyCtx &ctx )
{
  /* UNLINK key [key2 ...] */
  return this->exec_del( ctx );
}

ExecStatus
RedisExec::exec_wait( void )
{
  /* WAIT numslave timeout */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_scan( void )
{
  ScanArgs   sa;
  ExecStatus status;
  /* SCAN cursor [MATCH pat] [COUNT cnt] */
  if ( (status = this->match_scan_args( sa, 1 )) != EXEC_OK )
    return status;
  status = this->scan_keys( sa );
  this->release_scan_args( sa );
  return status;
}

ExecStatus
RedisExec::match_scan_args( ScanArgs &sa,  size_t i )
{
  const char * pattern = NULL;
  size_t       patlen  = 0;

  /* SCAN/HSCAN/SSCAN [key] cursor [MATCH pat] [COUNT cnt] */
  if ( ! this->msg.get_arg( i++, sa.pos ) )
    return ERR_BAD_ARGS;
  for ( ; i < this->argc; i += 2 ) {
    switch ( this->msg.match_arg( i, "match", 5,
                                     "count", 5, NULL ) ) {
      case 1:
        if ( ! this->msg.get_arg( i+1, pattern, patlen ) )
          return ERR_BAD_ARGS;
        break;
      case 2:
        if ( ! this->msg.get_arg( i+1, sa.maxcnt ) )
          return ERR_BAD_ARGS;
        break;
      default:
        return ERR_BAD_ARGS;
    }
  }
  if ( pattern != NULL ) {
    char       buf[ 1024 ];
    size_t     erroff;
    int        error;
    PatternCvt cvt( buf, sizeof( buf ) );

    if ( cvt.convert_glob( pattern, patlen ) != 0 )
      return ERR_BAD_ARGS;

    sa.re = pcre2_compile( (uint8_t *) buf, cvt.off, 0, &error, &erroff, 0 );
    if ( sa.re == NULL ) {
      return ERR_BAD_ARGS;
    }
    sa.md = pcre2_match_data_create_from_pattern( sa.re, NULL );
    if ( sa.md == NULL ) {
      pcre2_code_free( sa.re );
      sa.re = NULL;
      return ERR_BAD_ARGS;
    }
  }
  return EXEC_OK;
}

void
RedisExec::release_scan_args( ScanArgs &sa )
{
  if ( sa.re != NULL ) {
    pcre2_match_data_free( sa.md );
    pcre2_code_free( sa.re );
  }
}

ExecStatus
RedisExec::scan_keys( ScanArgs &sa )
{
  StreamBuf::BufQueue q( this->strm );
  size_t cnt = 0;
  int    rc  = 1; /* 1=matched when no regex */

  uint64_t ht_size = this->kctx.ht.hdr.ht_size;
  for ( ; (uint64_t) sa.pos < ht_size; sa.pos++ ) {
    KeyStatus status = this->kctx.fetch( &this->wrk, sa.pos, 0, true );
    if ( status == KEY_OK ) {
      KeyFragment *kp;
      status = this->kctx.get_key( kp );
      if ( status == KEY_OK ) {
        uint16_t keylen = kp->keylen;
        /* keys are null terminated */
        if ( keylen > 0 && kp->u.buf[ keylen - 1 ] == '\0' )
          keylen--;
        if ( sa.re != NULL )
          rc = pcre2_match( sa.re, (PCRE2_SPTR8) kp->u.buf, keylen, 0, 0,
                            sa.md, 0 );
        if ( rc > 0 ) {
          if ( q.append_string( kp->u.buf, keylen ) == 0 )
            return ERR_ALLOC_FAIL;
          cnt++;
          if ( --sa.maxcnt == 0 )
            goto break_loop;
        }
      }
    }
  }
break_loop:;
  q.finish_tail();
  if ( sa.maxcnt >= 0 ) {
    /* next cursor */
    sa.pos = ( ( (uint64_t) sa.pos == ht_size ) ? 0 : sa.pos + 1 );
    q.prepend_cursor_array( sa.pos, cnt );
  }
  else
    q.prepend_array( cnt );
  this->strm.append_iov( q );

  return EXEC_OK;
}

