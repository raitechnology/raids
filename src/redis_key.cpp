#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

using namespace rai;
using namespace ds;
using namespace kv;

ExecStatus
RedisExec::exec_del( RedisKeyCtx &ctx )
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
RedisExec::exec_dump( RedisKeyCtx &ctx )
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
          buf[ off ] = '\r';
          buf[ off + 1 ] = '\n';
          off += 2;
          ::memmove( &buf[ 32 - off ], buf, off );
          buf[ 32 + size ] = '\r';
          buf[ 32 + size + 1 ] = '\n';
          this->strm.append_iov( &buf[ 32 - off ], size + off + 2 );
          return EXEC_OK;
        }
      }
    }
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
  }
}

ExecStatus
RedisExec::exec_exists( RedisKeyCtx &ctx )
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
RedisExec::exec_expire( RedisKeyCtx &ctx )
{
  /* EXPIRE key secs */
  return this->do_pexpire( ctx, 1000 * 1000 * 1000 );
}

ExecStatus
RedisExec::exec_expireat( RedisKeyCtx &ctx )
{
  /* EXPIREAT key stamp */
  return this->do_pexpireat( ctx, 1000 * 1000 * 1000 );
}

ExecStatus
RedisExec::exec_keys( void )
{
  const char * pattern;
  size_t       patlen;
  /* KEYS pattern */
  if ( ! this->msg.get_arg( 1, pattern, patlen ) )
    return ERR_BAD_ARGS;
  return this->scan_keys( 0, -1, pattern, patlen );
}

ExecStatus
RedisExec::exec_migrate( RedisKeyCtx &ctx )
{
  /* MIGRATE host port key */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_move( RedisKeyCtx &ctx )
{
  /* MOVE key db# */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_object( RedisKeyCtx &ctx )
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
RedisExec::exec_persist( RedisKeyCtx &ctx )
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
RedisExec::exec_pexpire( RedisKeyCtx &ctx )
{
  /* PEXPIRE key ms */
  return this->do_pexpire( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pexpire( RedisKeyCtx &ctx,  uint64_t units )
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
RedisExec::exec_pexpireat( RedisKeyCtx &ctx )
{
  /* PEXPIREAT key stamp */
  return this->do_pexpireat( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pexpireat( RedisKeyCtx &ctx,  uint64_t units )
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
RedisExec::exec_pttl( RedisKeyCtx &ctx )
{
  /* PTTL key */
  return this->do_pttl( ctx, 1000 * 1000 );
}

ExecStatus
RedisExec::do_pttl( RedisKeyCtx &ctx,  int64_t units )
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

ExecStatus
RedisExec::exec_rename( RedisKeyCtx &ctx )
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
          ::memcpy( data, this->keys[ 0 ]->part->data, sz ); /* copy key data */
          this->kctx.set_type( this->keys[ 0 ]->type );
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
          this->save_data( ctx, data, sz );
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
RedisExec::exec_renamenx( RedisKeyCtx &ctx )
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
RedisExec::exec_restore( RedisKeyCtx &ctx )
{
  /* RESTORE key ttl value */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sort( RedisKeyCtx &ctx )
{
  /* SORT key [BY pat] [LIMIT off cnt] [GET pat] [ASC|DESC] 
   *          [ALPHA] [STORE dest] */
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_touch( RedisKeyCtx &ctx )
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
RedisExec::exec_ttl( RedisKeyCtx &ctx )
{
  /* TTL key */
  return this->do_pttl( ctx, 1000 * 1000 * 1000 );
}

ExecStatus
RedisExec::exec_type( RedisKeyCtx &ctx )
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
RedisExec::exec_unlink( RedisKeyCtx &ctx )
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
  const char * pattern = NULL;
  size_t       patlen  = 0;
  int64_t      maxcnt  = 10,
               pos;
  /* SCAN cursor [MATCH pattern] [COUNT count] */
  if ( ! this->msg.get_arg( 1, pos ) )
    return ERR_BAD_ARGS;
  for ( size_t i = 2; i < this->argc; ) {
    switch ( this->msg.match_arg( i, "match", 5,
                                     "count", 5, NULL ) ) {
      case 1:
        if ( ! this->msg.get_arg( i + 1, pattern, patlen ) )
          return ERR_BAD_ARGS;
        i += 2;
        break;
      case 2:
        if ( ! this->msg.get_arg( i + 1, maxcnt ) )
          return ERR_BAD_ARGS;
        i += 2;
        break;
      default:
        return ERR_BAD_ARGS;
    }
  }
  return this->scan_keys( pos, maxcnt, pattern, patlen );
}

ExecStatus
RedisExec::scan_keys( uint64_t pos,  int64_t maxcnt,  const char *pattern,
                      size_t patlen )
{
  StreamBuf::BufList
          * hd     = NULL,
          * tl     = NULL;
  char    * keybuf = NULL;
  uint8_t   buf[ 1024 ],
          * bf = buf;
  size_t    erroff,
            blen    = sizeof( buf ),
            cnt    = 0,
            buflen = 0,
            used   = 0;
  int       rc,
            error;
  pcre2_code       * re = NULL;
  pcre2_match_data * md = NULL;

  if ( patlen > 0 ) {
    rc = pcre2_pattern_convert( (PCRE2_SPTR8) pattern, patlen,
                                PCRE2_CONVERT_GLOB_NO_WILD_SEPARATOR,
                                &bf, &blen, 0 );
    if ( rc != 0 )
      return ERR_BAD_ARGS;
    re = pcre2_compile( bf, blen, 0, &error, &erroff, 0 );
    if ( re == NULL )
      return ERR_BAD_ARGS;
    md = pcre2_match_data_create_from_pattern( re, NULL );
    if ( md == NULL ) {
      pcre2_code_free( re );
      return ERR_BAD_ARGS;
    }
  }
  else {
    re = NULL;
    md = NULL;
    rc = 1;
  }
  uint64_t ht_size = this->kctx.ht.hdr.ht_size;
  for ( ; pos < ht_size; pos++ ) {
    KeyStatus status = this->kctx.fetch( &this->wrk, pos, 0, true );
    if ( status == KEY_OK ) {
      KeyFragment *kp;
      status = this->kctx.get_key( kp );
      if ( status == KEY_OK ) {
        uint16_t keylen = kp->keylen;
        if ( keylen > 0 && kp->u.buf[ keylen - 1 ] == '\0' )
          keylen--;
        if ( re != NULL )
          rc = pcre2_match( re, (PCRE2_SPTR8) kp->u.buf, keylen, 0, 0, md, 0 );
        if ( rc > 0 ) {
          if ( (size_t) keylen + 32 > buflen - used ) {
            if ( tl != NULL )
              tl->used = used;
            used   = 0;
            buflen = 2000;
            if ( buflen < (size_t) keylen + 32 )
              buflen = keylen + 32;
            tl = this->strm.alloc_buf_list( hd, tl, buflen );
            if ( tl == NULL )
              return ERR_ALLOC_FAIL;
            keybuf = tl->buf;
          }
          keybuf[ used ] = '$';
          used += 1 + RedisMsg::int_to_str( keylen, &keybuf[ used + 1 ] );
          keybuf[ used ] = '\r';
          keybuf[ used + 1 ] = '\n';
          ::memcpy( &keybuf[ used + 2 ], kp->u.buf, keylen );
          used += keylen + 2;
          keybuf[ used ] = '\r';
          keybuf[ used + 1 ] = '\n';
          used += 2;
          cnt++;
          if ( --maxcnt == 0 )
            goto break_loop;
        }
      }
    }
  }
break_loop:;
  if ( re != NULL ) {
    pcre2_match_data_free( md );
    pcre2_code_free( re );
  }
  if ( tl != NULL )
    tl->used = used;
  char *hdr = (char *) this->strm.alloc_temp( 64 );
  if ( hdr == NULL )
    return ERR_ALLOC_FAIL;
  /* cursor usage */
  if ( maxcnt >= 0 ) {
    pos = ( ( pos == ht_size ) ? 0 : pos + 1 ); /* next cursor */
    /* construct [cursor, [key, ...]] */
    ::strcpy( hdr, "*2\r\n$" );
    size_t len = RedisMsg::uint_digits( pos );
    used = 5 + RedisMsg::uint_to_str( len, &hdr[ 5 ] );
    hdr[ used ] = '\r';
    hdr[ used + 1 ] = '\n';
    used += 2;
    used += RedisMsg::uint_to_str( pos, &hdr[ used ], len );
    hdr[ used ] = '\r';
    hdr[ used + 1 ] = '\n';
    used += 2;
  }
  else {
    /* construct [key, ...] */
    used = 0;
  }
  hdr[ used ] = '*';
  used += 1 + RedisMsg::int_to_str( cnt, &hdr[ used + 1 ] );
  hdr[ used ] = '\r';
  hdr[ used + 1 ] = '\n';
  this->strm.append_iov( hdr, used + 2 );
  while ( hd != NULL ) {
    if ( hd->used > 0 )
      this->strm.append_iov( hd->buf, hd->used );
    hd = hd->next;
  }

  return EXEC_OK;
}

