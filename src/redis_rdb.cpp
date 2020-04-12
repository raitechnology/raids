#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <raimd/md_types.h>
#include <raids/redis_exec.h>
#include <raids/redis_rdb.h>
#include <raids/exec_list_ctx.h>
#include <raids/exec_stream_ctx.h>
#include <h3api.h>

using namespace rai;
using namespace ds;
using namespace md;
using namespace kv;
using namespace rdbparser;


ExecStatus
RedisExec::exec_dump( EvKeyCtx &ctx ) noexcept
{
  /* DUMP key (serialize key) */
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_OK: {
      switch ( this->kctx.get_type() ) {
        default:
        case MD_STRING: return this->dump_string( ctx );
        case MD_LIST:   return this->dump_list( ctx );
        case MD_HASH:   return this->dump_hash( ctx );
        case MD_SET:    return this->dump_set( ctx );
        case MD_ZSET:   return this->dump_zset( ctx );
        case MD_GEO:    return this->dump_geo( ctx );
        case MD_HYPERLOGLOG:
                        return this->dump_hll( ctx );
        case MD_STREAM: return this->dump_stream( ctx );
      }
    }
    /* FALLTHRU */
    default:            return ERR_KV_STATUS;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
  }
}

static int
mk_aux( char *out,  const char *name,  int64_t val )
{
  RdbLenEncode rdb;   /* if save, this is the encoded key length */
  out[ 0 ] = RDB_AUX;
  int i = rdb.str_size( ::strlen( name ) );
  rdb.str_encode( &out[ 1 ], name );
  int j = rdb.int_size( val );
  rdb.int_encode( &out[ i + 1 ] );
  return 1 + i + j;
}

ExecStatus
RedisExec::exec_save( void ) noexcept
{
  static const char ver[] = "REDIS0009";
  uint64_t     ht_size  = this->kctx.ht.hdr.ht_size,
               pos;
  size_t       strm_idx = 0,
               nbytes;
  int          fd = ::open( "dump.rdb", O_CREAT | O_TRUNC | O_WRONLY, 0666 );
  ExecStatus   status;
  char         ctime_aux[ 32 ],
               aof_aux[ 32 ];
  uint64_t     cur_time = this->kctx.ht.hdr.current_stamp;
  int          ctime_sz = mk_aux( ctime_aux, "ctime", cur_time / 1000000000 ),
               aof_sz   = mk_aux( aof_aux, "aof-preamble", 0 ),
               db_sz;
  uint8_t      db_select[ 8 ];
  RdbLenEncode rdb;
  /* construct hdr */
  db_select[ 0 ] = RDB_DBSELECT;
  db_sz = 1 + rdb.len_encode( &db_select[ 1 ], this->kctx.db_num );
  if ( fd < 0 ||
       ::write( fd, ver, 9 ) != 9 ||
       ::write( fd, ctime_aux, ctime_sz ) != ctime_sz ||
       ::write( fd, aof_aux, aof_sz ) != aof_sz ||
       ::write( fd, db_select, db_sz ) != db_sz ) {
    perror( "dump.rdb" );
    if ( fd >= 0 )
      ::close( fd );
    return ERR_SAVE;
  }
  this->strm.reset();
  this->cmd_state |= CMD_STATE_SAVE;
  /* update crc */
  EvKeyCtx key( this->kctx.ht );
  key.ival = rdbparser::jones_crc64( 0, ver, 9 );
  key.ival = rdbparser::jones_crc64( key.ival, ctime_aux, ctime_sz );
  key.ival = rdbparser::jones_crc64( key.ival, aof_aux, aof_sz );
  key.ival = rdbparser::jones_crc64( key.ival, db_select, db_sz );
  /* for each position in ht, convert key to rdb format */
  for ( pos = 0; pos < ht_size; pos++ ) {
    for (;;) {
      status = ERR_KV_STATUS;
      key.kstatus = this->kctx.fetch( &this->wrk, pos, true );
      if ( key.kstatus == KEY_OK ) {
        this->save_key = NULL;
        key.kstatus = this->kctx.get_key( this->save_key );
        if ( key.kstatus == KEY_OK ) {
          this->kctx.get_stamps( key.hash1 /*exp*/, key.hash2 /*upd*/);
          if ( key.hash1 != 0 )
            key.hash1 /= 1000000; /* in millisecs */
          if ( key.hash2 != 0 )
            key.hash2 = ( cur_time - key.hash2 ) / 1000000000; /* in seconds */
          status = this->exec_dump( key );
        }
      }
      if ( key.kstatus != KEY_MUTATED || status != ERR_KV_STATUS )
        break;
    }
    nbytes = this->strm.pending() - this->strm_start;
    if ( nbytes >= 16 * 1024 ) {
      this->strm.flush();
      if ( ::writev( fd, &this->strm.iov[ strm_idx ],
                     this->strm.idx - strm_idx ) != (ssize_t) nbytes ) {
        status = ERR_SAVE;
        goto break_loop;
      }
      this->strm.reset();
    }
  }
  status = EXEC_SEND_OK;
  nbytes = this->strm.pending() - this->strm_start;
  if ( nbytes > 0 ) {
    static const uint8_t eof = 0xff;
    key.ival = rdbparser::jones_crc64( key.ival, &eof, 1 );
    this->strm.flush();
    if ( ::writev( fd, &this->strm.iov[ strm_idx ],
                   this->strm.idx - strm_idx ) != (ssize_t) nbytes ||
         ::write( fd, &eof, 1 ) != 1 ||     /* terminate dump */
         ::write( fd, &key.ival, 8 ) != 8 ) /* crc */
      status = ERR_SAVE;
  }
break_loop:;
  this->strm.reset();
  this->cmd_state &= ~CMD_STATE_SAVE;
  this->save_key = NULL;
  ::close( fd );
  return status;
}

namespace {
struct RdbDumpGeom {
  KeyFragment * key;
  size_t        sz,    /* alloc size of message */
                start, /* where to start encoding rdb type */
                len,   /* length of data minus type, version crc */
                digs,  /* digits in length */
                trail; /* when dump, this is offset where ver crc goes */
  uint64_t      crc,   /* crc for save */
                expires;
  RdbType       type;  /* codec type */
  RdbLenEncode  rdb,   /* if save, this is the encoded key length */
                idle;  /* if save, this can be idle time */

  RdbDumpGeom( RdbType t,  size_t pack_size,  KeyFragment *kp, EvKeyCtx &ctx ) {
    this->type = t;
    this->key  = kp;
    if ( kp != NULL ) {
      this->start = 1 + this->rdb.str_size( kp->keylen - 1 );
      if ( ctx.hash1 != 0 ) { /* expires */
        this->start += 9;
        this->expires = ctx.hash1;
      }
      else
        this->expires = 0;
      if ( ctx.hash2 != 0 )   /* updated */
        this->start += 1 + this->idle.len_size( ctx.hash2 );
      else
        this->idle.init();
      this->sz    = this->start + pack_size;
      this->len   = 0; /* these are unused */
      this->digs  = 0;
      this->trail = 0;
      this->crc   = ctx.ival;
    }
    else {
      this->len     = 1 + pack_size + 2 + 8;    /* S data 9 0 crc */
      this->digs    = uint_digits( this->len ); /* $ nnn <- digits in return */
      this->sz      = 1 + this->digs + 2 + this->len + 2;/* $ nnn\r\n len \r\n*/
      this->trail   = 1 + this->digs + 2 + 1 + pack_size;
      this->start   = 1 + this->digs + 3;
      this->crc     = 0;
      this->expires = 0;
      this->rdb.init();
      this->idle.init();
    }
  }

  int64_t frame_dump_result( char *p ) noexcept;
};

inline int64_t
RdbDumpGeom::frame_dump_result( char *p ) noexcept
{
  if ( this->key != NULL ) {
    size_t off = 0;
    if ( this->expires != 0 ) {
      p[ off++ ] = RDB_EXPIRED_MS;
      le<uint64_t>( &p[ off ], this->expires );
      off += 8;
    }
    if ( this->idle.lcode != RdbLength::RDB_LEN_ERR ) {
      p[ off++ ] = RDB_IDLE;
      off += this->idle.len_encode( &p[ off ] );
    }
    p[ off++ ] = this->type;
    this->rdb.str_encode( &p[ off ], this->key->u.buf );
    this->crc = rdbparser::jones_crc64( this->crc, p, this->sz );
  }
  else {
    size_t off = this->start - 1;
    p[ 0 ] = '$';
    uint_to_str( this->len, &p[ 1 ], this->digs ); /* $ nnn */
    crlf( p, this->digs + 1 );
    p[ off ] = this->type;
    size_t i = this->trail;
    p[ i++ ] = 9;
    p[ i++ ] = 0;
    rdbparser::le<uint64_t>( (uint8_t *) &p[ i ],
      rdbparser::jones_crc64( 0, &p[ off ], i - off ) );
    crlf( p, i + 8 );
  }
  return this->crc;
}
}

ExecStatus
RedisExec::dump_string( EvKeyCtx &ctx ) noexcept
{
  void * data;
  size_t size;
  switch ( this->exec_key_fetch( ctx ) ) {
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:
      if ( (ctx.kstatus = this->kctx.value( &data, size )) == KEY_OK )
        break;
      /* FALLTHRU */
    default: return ERR_KV_STATUS;
  }

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_STRING, rdb.str_size( size ), this->save_key, ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  rdb.str_encode( &p[ dump.start ], data );  /* key value */

  ctx.kstatus = this->kctx.validate_value(); /* check if key is valid */
  if ( ctx.kstatus != KEY_OK )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_list( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<ListData, MD_LIST> list( *this, ctx );

  switch ( list.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! list.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = list.x->count();
  ListStatus   lstatus = LIST_OK;
  ListVal      lv;
  RdbZipEncode zip;

  zip.init();
  for ( i = 0; i < count; i++ ) {
    lstatus = list.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    zip.calc_link( lv.sz + lv.sz2 );
  }
  zip.calc_end();

  if ( lstatus != LIST_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_LIST_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 0; i < count; i++ ) {
    lstatus = list.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    zip.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
  }
  zip.append_end( count );

  if ( ! list.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_hash( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<HashData, MD_HASH> hash( *this, ctx );

  switch ( hash.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! hash.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = hash.x->count();
  HashStatus   hstatus = HASH_OK;
  HashVal      hv;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    hstatus = hash.x->hindex( i, hv );
    if ( hstatus != HASH_OK )
      break;
    zip.calc_link( hv.keylen );
    zip.calc_link( hv.sz + hv.sz2 );
  }
  zip.calc_end();

  if ( hstatus != HASH_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_HASH_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    hstatus = hash.x->hindex( i, hv );
    if ( hstatus != HASH_OK )
      break;
    zip.append_link( hv.key, hv.keylen );
    zip.append_link( hv.data, hv.data2, hv.sz, hv.sz2 );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! hash.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_set( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<SetData, MD_SET> set( *this, ctx );

  switch ( set.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! set.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = set.x->count();
  ListStatus   lstatus = LIST_OK;
  ListVal      lv;
  RdbLenEncode rdb;
  size_t       len = 0;

  for ( i = 1; i < count; i++ ) {
    lstatus = set.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    len += rdb.str_size( lv.sz + lv.sz2 );
  }

  if ( lstatus != LIST_OK )
    return EXEC_SEND_NIL;

  RdbDumpGeom dump( RDB_SET, len + rdb.len_size( count - 1 ), this->save_key,
                    ctx );
  size_t      off = dump.start;
  char      * p   = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  off += rdb.len_encode( &p[ off ] ); /* the count */

  for ( i = 1; i < count; i++ ) {
    lstatus = set.x->lindex( i, lv );
    if ( lstatus != LIST_OK )
      break;
    rdb.str_size( lv.sz + lv.sz2 );
    off += rdb.len_encode( &p[ off ] );
    ::memcpy( &p[ off ], lv.data, lv.sz );
    off += lv.sz;
    if ( lv.sz2 > 0 ) {
      ::memcpy( &p[ off ], lv.data2, lv.sz2 );
      off += lv.sz2;
    }
  }

  if ( ! set.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_zset( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<ZSetData, MD_ZSET> zset( *this, ctx );

  switch ( zset.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! zset.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = zset.x->count();
  ZSetStatus   zstatus = ZSET_OK;
  ZSetVal      zv;
  char         fpdata[ 64 ];
  size_t       fplen;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    zstatus = zset.x->zindex( i, zv );
    if ( zstatus != ZSET_OK )
      break;
    zip.calc_link( zv.sz + zv.sz2 );
    zip.calc_link( zv.score.to_string( fpdata ) );
  }
  zip.calc_end();

  if ( zstatus != ZSET_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_ZSET_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    zstatus = zset.x->zindex( i, zv );
    if ( zstatus != ZSET_OK )
      break;
    zip.append_link( zv.data, zv.data2, zv.sz, zv.sz2 );
    fplen = zv.score.to_string( fpdata );
    zip.append_link( fpdata, fplen );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! zset.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_geo( EvKeyCtx &ctx ) noexcept
{
  ExecListCtx<GeoData, MD_GEO> geo( *this, ctx );

  switch ( geo.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! geo.open_readonly() )
    return ERR_KV_STATUS;

  size_t       i,
               count   = geo.x->count();
  GeoStatus    gstatus = GEO_OK;
  GeoVal       gv;
  char         fpdata[ 64 ];
  size_t       fplen;
  RdbZipEncode zip;

  zip.init();
  for ( i = 1; i < count; i++ ) {
    gstatus = geo.x->geoindex( i, gv );
    if ( gstatus != GEO_OK )
      break;
    zip.calc_link( gv.sz + gv.sz2 );
    zip.calc_link( uint_digits( gv.score ) );
  }
  zip.calc_end();

  if ( gstatus != GEO_OK )
    return EXEC_SEND_NIL;

  RdbLenEncode rdb;
  RdbDumpGeom  dump( RDB_ZSET_ZIPLIST, rdb.str_size( zip.off ), this->save_key,
                     ctx );
  char       * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  i = rdb.len_encode( &p[ dump.start ] );

  zip.init( &p[ dump.start + i ] );
  for ( i = 1; i < count; i++ ) {
    gstatus = geo.x->geoindex( i, gv );
    if ( gstatus != GEO_OK )
      break;
    zip.append_link( gv.data, gv.data2, gv.sz, gv.sz2 );
    fplen = uint_digits( gv.score );
    uint_to_str( gv.score, fpdata, fplen ); /* $ nnn */
    zip.append_link( fpdata, fplen );
  }
  zip.append_end( ( count - 1 ) * 2 );

  if ( ! geo.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

ExecStatus
RedisExec::dump_hll( EvKeyCtx &ctx ) noexcept
{
  return this->dump_string( ctx );
}

ExecStatus
RedisExec::dump_stream( EvKeyCtx &ctx ) noexcept
{
  struct Consumers {
    ListVal  lv;
    uint64_t pending_cnt,
             last_seen;
  };
  ExecStreamCtx     stream( *this, ctx );
  MDMsgMem          tmp;
  ListData          ld;
  ListVal           lv, lv_grp, lv_id, lv_ns, lv_cnt;
  size_t            i,
                    j,
                    k,
                    n,
                    off,
                    scnt,
                    fcnt,
                    gcnt,
                    pcnt,
                    pending_count,
                    num_consumers,
                    max_consumers = 0;
  Consumers       * con = NULL;
  RdbListPackEncode lst;
  StreamId          sid;
  uint64_t          start_x = 0,
                    start_y = 0,
                    upd, exp,
                    last_ms,
                    last_ser;
  RdbLenEncode      rdb;

  switch ( stream.get_key_read() ) {
    default:            return ERR_KV_STATUS;
    case KEY_NO_VALUE:  return ERR_BAD_TYPE;
    case KEY_NOT_FOUND: return EXEC_SEND_NIL;
    case KEY_OK:        break;
  }
  if ( ! stream.open_readonly() )
    return ERR_KV_STATUS;

  scnt = stream.x->stream.count();
  gcnt = stream.x->group.count();
  pcnt = stream.x->pending.count();
  lst.init();
  lst.calc_immediate_int( scnt ); /* stream entry count */
  lst.calc_immediate_int( 0 );      /* deleted entry count */
  lst.calc_immediate_int( 0 );      /* master field count */
  lst.calc_immediate_int( 0 );      /* back link count */
  for ( i = 0; i < scnt; i++ ) {
    if ( stream.x->sindex( stream.x->stream, i, ld, tmp ) != STRM_OK )
      break;
    fcnt = ld.count();
    if ( fcnt < 3 || ld.lindex( 0, lv ) != LIST_OK ||
         ! sid.str_to_id( lv, tmp ) )
      break;
    /* "entries" : [
        { "id" : "1585292469991-0", "loc" : "mel", "temp" : "23" },
        { "id" : "1585292477471-0", "loc" : "sfo", "temp" : "17" } ], */
    lst.calc_immediate_int( 0 ); /* master flds not used */
    if ( i == 0 ) {
      lst.calc_immediate_int( 0 );   /* ms offset */
      lst.calc_immediate_int( 0 );   /* ser offset */
      start_x = sid.x;
      start_y = sid.y;
    }
    else {
      lst.calc_immediate_int( sid.x - start_x );   /* ms offset */
      lst.calc_immediate_int( sid.y - start_y );   /* ser offset */
    }
    fcnt -= 1;
    n = fcnt / 2;
    fcnt = n * 2; /* fcnt must be even */
    lst.calc_immediate_int( n );
    for ( k = 1; ; ) {
      if ( n-- == 0 )
        break;
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.calc_link( lv.sz + lv.sz2 );
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.calc_link( lv.sz + lv.sz2 );
    }
    lst.calc_immediate_int( 4 + fcnt ); /* back link count */
  }
  lst.calc_end();
  off = rdb.len_size( scnt ); /* count of stream recs */
  this->kctx.get_stamps( exp, upd );
  last_ms  = upd / 1000000;
  last_ser = upd % 1000000;
  /* "last_id" : "1585292477471-0",
     "num_elems" : 2,
     "num_cgroups" : 2 */
  off += rdb.len_size( last_ms );  /* last ms id used */
  off += rdb.len_size( last_ser ); /* last serial used */
  off += rdb.len_size( gcnt ); /* number of groups */
  for ( i = 0; i < gcnt; i++ ) {
    if ( stream.x->sindex( stream.x->group, i, ld, tmp ) != STRM_OK )
      break;
    if ( ld.lindex( 0, lv_grp ) != LIST_OK ||
         ld.lindex( 1, lv_id ) != LIST_OK ||
         ! sid.str_to_id( lv_id, tmp ) )
      break;
    /* "group": "strgrp",               <- rdb str
       "pending": 2,                    <- rdb len
       "last_id": "1585458175186-0",    <- rdb len * 2 */
    off += rdb.str_size( lv_grp.sz + lv_grp.sz2 ); /* group name */
    off += rdb.len_size( sid.x );            /* ms */
    off += rdb.len_size( sid.y );            /* ser */

    pending_count = 0;
    num_consumers = 0;

    for ( j = 0; j < pcnt; j++ ) {
      if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
        break;
      if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
        pending_count++;
        if ( ld.lindex( P_CON, lv ) != LIST_OK ||
             ld.lindex( P_NS, lv_ns ) != LIST_OK ||
             ld.lindex( P_CNT, lv_cnt ) != LIST_OK )
          break;
        for ( k = 0; k < num_consumers; k++ ) {
          if ( lv.cmp_key( con[ k ].lv ) == 0 )
            break;
        }
        /* add new consumer to list */
        if ( k == num_consumers ) {
          if ( k == max_consumers ) {
            tmp.extend( sizeof( con[ 0 ] ) * i,
                        sizeof( con[ 0 ] ) * ( k + 8 ), &con );
            max_consumers = k + 8;
          }
          con[ k ].lv = lv;
          con[ k ].pending_cnt = 1;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
          num_consumers++;
        }
        else {
          con[ k ].pending_cnt++;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
        }
        off += rdb.len_size( lv_cnt.u32() );
      }
    }
    /* "pel": [
         {
           "id": "1585458171466-0",     <- 16 bytes big endian
           "last_d": 1585468380773,     <-  8 bytes little endian
           "d_cnt": 1                   <-  rdb len
         },
       ],
       "consumers": [
          {
            "name": "strcons1",         <- rdb str
            "pending": 2,               <- rdb len
            "last_seen": 1585468380773, <-  8 bytes little endian
            "pel": [
              "1585458171466-0",        <- 16 bytes big endian
              "1585458175186-0"
            ]
          }
        ] */
    if ( pending_count > 0 ) {
      off += rdb.len_size( pending_count ); /* pending */
      off += pending_count * ( 16 + 8 ) ; /* stream id + last deliv */
    }
    else {
      off += rdb.len_size( 0 );
    }
    if ( num_consumers > 0 ) {
      off += rdb.len_size( num_consumers ); /* consumers */
      for ( j = 0; j < num_consumers; j++ ) {
        off += rdb.str_size( con[ j ].lv.sz + con[ j ].lv.sz2 );
        off += rdb.len_size( con[ j ].pending_cnt );
        off += con[ j ].pending_cnt * 16; /* stream id pel */
        off += 8; /* last seen */
      }
    }
    else {
      off += rdb.len_size( 0 );
    }
  }

  RdbDumpGeom dump( RDB_STREAM_LISTPACK, 1 /* Lpcnt */ + 17 /* key */ +
                    rdb.str_size( lst.off ) + off, this->save_key, ctx );
  char      * p = this->strm.alloc( dump.sz );
  if ( p == NULL )
    return ERR_ALLOC_FAIL;

  p[ dump.start ]     = 1; /* stream listpack count */
  p[ dump.start + 1 ] = 16;

  off = rdb.len_encode( &p[ dump.start + 18 ] );
  lst.init( &p[ dump.start + 18 + off ] );
  lst.append_immediate_int( scnt ); /* stream entry count */
  lst.append_immediate_int( 0 );      /* deleted entry count */
  lst.append_immediate_int( 0 );      /* master field count */
  lst.append_immediate_int( 0 );      /* back link count */

  for ( i = 0; i < scnt; i++ ) {
    if ( stream.x->sindex( stream.x->stream, i, ld, tmp ) != STRM_OK )
      break;
    fcnt = ld.count();
    if ( fcnt < 3 || ld.lindex( 0, lv ) != LIST_OK ||
         ! sid.str_to_id( lv, tmp ) )
      break;
    /* "entries" : [
        { "id" : "1585292469991-0", "loc" : "mel", "temp" : "23" },
        { "id" : "1585292477471-0", "loc" : "sfo", "temp" : "17" } ], */
    lst.append_immediate_int( 0 );   /* master flds not used */
    if ( i == 0 ) {
      be<uint64_t>( &p[ dump.start + 2 ], sid.x );
      be<uint64_t>( &p[ dump.start + 8 + 2 ], sid.y );
      lst.append_immediate_int( 0 );   /* ms offset */
      lst.append_immediate_int( 0 );   /* ser offset */
      start_x = sid.x;
      start_y = sid.y;
    }
    else {
      lst.append_immediate_int( sid.x - start_x );   /* ms offset */
      lst.append_immediate_int( sid.y - start_y );   /* ser offset */
    }
    fcnt -= 1;
    n = fcnt / 2;
    fcnt = n * 2; /* fcnt must be even */
    lst.append_immediate_int( n );
    for ( k = 1; ; ) {
      if ( n-- == 0 )
        break;
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
      if ( ld.lindex( k++, lv ) == LIST_OK )
        lst.append_link( lv.data, lv.data2, lv.sz, lv.sz2 );
    }
    lst.append_immediate_int( 4 + fcnt ); /* back link count */
  }
  lst.append_end();

  char * q = (char *) lst.p;
  off  = lst.off;
  off += rdb.len_encode( &q[ off ], scnt );   /* count of stream recs */
  /* "last_id" : "1585292477471-0",
     "num_elems" : 2,
     "num_cgroups" : 2 */
  off += rdb.len_encode( &q[ off ], last_ms );  /* last ms id used */
  off += rdb.len_encode( &q[ off ], last_ser ); /* last serial used */
  off += rdb.len_encode( &q[ off ], gcnt );   /* number of groups */
  for ( i = 0; i < gcnt; i++ ) {
    if ( stream.x->sindex( stream.x->group, i, ld, tmp ) != STRM_OK )
      break;
    if ( ld.lindex( 0, lv_grp ) != LIST_OK ||
         ld.lindex( 1, lv_id ) != LIST_OK ||
         ! sid.str_to_id( lv_id, tmp ) )
      break;
    /* "group": "strgrp",               <- rdb str
       "pending": 2,                    <- rdb len
       "last_id": "1585458175186-0",    <- rdb len * 2 */
    off += rdb.str_encode( &q[ off ], lv_grp.data, lv_grp.data2,
                                      lv_grp.sz, lv_grp.sz2 );
    off += rdb.len_encode( &q[ off ], sid.x ); /* last ms */
    off += rdb.len_encode( &q[ off ], sid.y ); /* last ser */
    pending_count = 0;
    num_consumers = 0;

    for ( j = 0; j < pcnt; j++ ) {
      if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
        break;
      if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
        pending_count++;
        if ( ld.lindex( P_CON, lv ) != LIST_OK ||
             ld.lindex( P_NS, lv_ns ) != LIST_OK ||
             ld.lindex( P_CNT, lv_cnt ) != LIST_OK )
          break;
        for ( k = 0; k < num_consumers; k++ ) {
          if ( lv.cmp_key( con[ k ].lv ) == 0 )
            break;
        }
        /* add new consumer to list */
        if ( k == num_consumers ) {
          if ( k == max_consumers ) {
            tmp.extend( sizeof( con[ 0 ] ) * i,
                        sizeof( con[ 0 ] ) * ( k + 8 ), &con );
            max_consumers = k + 8;
          }
          con[ k ].lv = lv;
          con[ k ].pending_cnt = 1;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
          num_consumers++;
        }
        else {
          con[ k ].pending_cnt++;
          con[ k ].last_seen = lv_ns.u64() / 1000000;
        }
      }
    }
    /* "pel": [
         {
           "id": "1585458171466-0",     <- 16 bytes big endian
           "last_d": 1585468380773,     <-  8 bytes little endian
           "d_cnt": 1                   <-  rdb len
         },
       ],
       "consumers": [
          {
            "name": "strcons1",         <- rdb str
            "pending": 2,               <- rdb len
            "last_seen": 1585468380773, <-  8 bytes little endian
            "pel": [
              "1585458171466-0",        <- 16 bytes big endian
              "1585458175186-0"
            ]
          }
        ] */
    if ( pending_count > 0 ) {
      off += rdb.len_encode( &q[ off ], pending_count ); /* pending */
      for ( j = 0; j < pcnt && pending_count > 0; j++ ) {
        if ( stream.x->sindex( stream.x->pending, j, ld, tmp ) != STRM_OK )
          break;
        if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK ) {
          pending_count -= 1;
          if ( ld.lindex( P_ID, lv_id ) != LIST_OK ||
               ld.lindex( P_NS, lv_ns ) != LIST_OK ||
               ld.lindex( P_CNT, lv_cnt ) != LIST_OK ||
               ! sid.str_to_id( lv_id, tmp ) )
            break;
          be<uint64_t>( &q[ off ], sid.x );
          be<uint64_t>( &q[ off + 8 ], sid.y );
          off += 16;
          le<uint64_t>( &q[ off ], lv_ns.u64() / 1000000 );
          off += 8;
          off += rdb.len_encode( &q[ off ], lv_cnt.u32() );
        }
      }
    }
    else {
      off += rdb.len_encode( &q[ off ], 0 );     /* pending cnt */
    }
    if ( num_consumers > 0 ) {
      off += rdb.len_encode( &q[ off ], num_consumers ); /* consumers */
      for ( j = 0; j < num_consumers; j++ ) {
        ListVal & lv_c = con[ j ].lv;
        off += rdb.str_encode( &q[ off ], lv_c.data, lv_c.data2,
                                          lv_c.sz, lv_c.sz2 );
        le<uint64_t>( &q[ off ], con[ j ].last_seen );
        off += 8;
        off += rdb.len_encode( &q[ off ], con[ j ].pending_cnt );

        pending_count = con[ j ].pending_cnt;
        for ( k = 0; k < pcnt && pending_count > 0; k++ ) {
          if ( stream.x->sindex( stream.x->pending, k, ld, tmp ) != STRM_OK )
            break;
          if ( ld.lindex_cmp_key( P_GRP, lv_grp ) == LIST_OK &&
               ld.lindex_cmp_key( P_CON, lv_c ) == LIST_OK &&
               ld.lindex( P_ID, lv_id ) == LIST_OK &&
               sid.str_to_id( lv_id, tmp ) ) {
            pending_count -= 1;
            be<uint64_t>( &q[ off ], sid.x );
            be<uint64_t>( &q[ off + 8 ], sid.y );
            off += 16;
          }
        }
      }
    }
    else {
      off += rdb.len_encode( &q[ off ], 0 );
    }
  }

  if ( ! stream.validate_value() )
    return ERR_KV_STATUS;

  ctx.ival = dump.frame_dump_result( p );
  this->strm.sz += dump.sz;
  return EXEC_OK;
}

void
ExecRestore::d_start_key( void ) noexcept
{
  this->tmp.reuse();
  if ( this->dec.is_rdb_file ) { /* has key */
    ExecReStrBuf rsb( this->dec.key );
    void *p;
    this->tmp.alloc( EvKeyCtx::size( rsb.len ), &p );
    this->keyp = new ( p ) EvKeyCtx( this->exec.kctx.ht, NULL, rsb.s, rsb.len,
                                     0, 0, this->exec.hs );
  }
  switch ( this->dec.type ) {
    default:
    case RDB_STRING:
    case RDB_MODULE_2:
      break;
    case RDB_LIST:
    case RDB_LIST_ZIPLIST:
    case RDB_LIST_QUICKLIST:
      this->list = NULL;
      break;
    case RDB_SET:
    case RDB_SET_INTSET:
      this->set = NULL;
      break;
    case RDB_ZSET:
    case RDB_ZSET_2:
    case RDB_ZSET_ZIPLIST:
      this->zset = NULL;
      this->geo  = NULL;
      this->is_geo = false;
      break;
    case RDB_HASH:
    case RDB_HASH_ZIPMAP:
    case RDB_HASH_ZIPLIST:
      this->hash = NULL;
      break;
    case RDB_STREAM_LISTPACK:
      this->strm    = NULL;
      this->pend    = NULL;
      this->pcount  = 0;
      this->last_ns = 0;
      break;
  }
}

void
ExecRestore::set_value( uint8_t type,  uint16_t fl,  const void *value,
                        size_t len ) noexcept
{
  EvKeyCtx & ctx = *this->keyp;
  void * data;

  switch ( this->exec.get_key_write( ctx, type ) ) {
    case KEY_NO_VALUE: /* overwrite key */
    case KEY_IS_NEW:
      ctx.flags |= EKF_IS_NEW;
      ctx.type   = type;
      /* FALLTHRU */
    case KEY_OK:
      /* replace must be set if key is not new */
      if ( this->is_replace && ! ctx.is_new() ) {
        this->status = ERR_KEY_EXISTS;
        return;
      }
      this->exec.kctx.clear_stamps( true, false );
      ctx.kstatus = this->exec.kctx.resize( &data, len );
      if ( ctx.kstatus == KEY_OK ) {
        ::memcpy( data, value, len );
        ctx.flags |= EKF_KEYSPACE_EVENT | fl;
        if ( this->ttl_ns + this->last_ns != 0 )
          this->exec.kctx.update_stamps( this->ttl_ns, this->last_ns );
        return;
      }
      /* FALLTHRU */
    default: this->status = ERR_KV_STATUS;
  }
}

void
ExecRestore::set_value( void ) noexcept
{
  switch ( this->dec.type ) {
    case RDB_LIST:
    case RDB_LIST_ZIPLIST:
    case RDB_LIST_QUICKLIST:
      if ( this->list != NULL ) {
        this->set_value( MD_LIST, EKF_KEYSPACE_LIST, this->list->listp,
                         this->list->size );
        return;
      }
      break;

    case RDB_SET:
    case RDB_SET_INTSET:
      if ( this->set != NULL ) {
        this->set_value( MD_SET, EKF_KEYSPACE_SET, this->set->listp,
                         this->set->size );
        return;
      }
      break;

    case RDB_ZSET:
    case RDB_ZSET_2:
    case RDB_ZSET_ZIPLIST:
      if ( ! this->is_geo ) {
        if ( this->zset != NULL ) {
          this->set_value( MD_ZSET, EKF_KEYSPACE_ZSET, this->zset->listp,
                           this->zset->size );
          return;
        }
      }
      else {
        if ( this->geo != NULL ) {
          this->set_value( MD_GEO, EKF_KEYSPACE_GEO, this->geo->listp,
                           this->geo->size );
          return;
        }
      }
      break;

    case RDB_HASH:
    case RDB_HASH_ZIPMAP:
    case RDB_HASH_ZIPLIST:
      if ( this->hash != NULL ) {
        this->set_value( MD_HASH, EKF_KEYSPACE_HASH, this->hash->listp,
                         this->hash->size );
        return;
      }
      break;

    case RDB_STREAM_LISTPACK:
      if ( this->strm != NULL ) {
        this->set_value( MD_STREAM, EKF_KEYSPACE_STREAM,
                         this->strm->stream.listp, this->strm->stream.size +
                         this->strm->group.size + this->strm->pending.size );
        return;
      }
      break;

    case RDB_STRING: return;

    case RDB_MODULE:  /* don't know how to decode these */
    case RDB_MODULE_2:
    default: break;
  }
  this->status = ERR_BAD_TYPE;
}

void
ExecRestore::d_end_key( void ) noexcept
{
  this->set_value();
  if ( this->dec.is_rdb_file ) {
    EvKeyCtx & ctx = *this->keyp;
    if ( ctx.is_new() ) {
      if ( ctx.type != MD_NODATA )
        this->exec.kctx.set_type( ctx.type );
      this->exec.kctx.set_val( 0 );
    }
    if ( this->last_ns == 0 )
      this->exec.kctx.update_stamps( 0, this->exec.kctx.ht.hdr.current_stamp );
    this->exec.kctx.release();
    this->reset_meta();
    this->keyp = NULL;
  }
}

void
ExecRestore::d_finish( bool ) noexcept
{
  if ( this->dec.is_rdb_file && this->keyp != NULL ) {
    this->exec.kctx.release();
    this->keyp = NULL;
  }
}

void
ExecRestore::d_idle( uint64_t i ) noexcept {
  if ( this->keyp != NULL )
    this->keyp->state |= EKS_NO_UPDATE;
  this->last_ns = (uint64_t) i * (uint64_t) 1000000000;
}
void
ExecRestore::d_freq( uint8_t f ) noexcept {
  this->freq = f;
}
void
ExecRestore::d_aux( const RdbString &/*var*/,
                    const RdbString &/*val*/ ) noexcept {}
void
ExecRestore::d_dbresize( uint64_t/*i*/,  uint64_t/*j*/) noexcept {}

void
ExecRestore::d_expired_ms( uint64_t ms ) noexcept {
  this->ttl_ns = (uint64_t) ms * (uint64_t) 1000000;
}
void
ExecRestore::d_expired( uint32_t sec ) noexcept {
  this->ttl_ns = (uint64_t) sec * (uint64_t) 1000000000;
}
void
ExecRestore::d_dbselect( uint32_t /*db*/ ) noexcept {
}

void
ExecRestore::d_string( const RdbString &str ) noexcept
{
  ExecReStrBuf rsb( str );
  if ( rsb.len == sizeof( HyperLogLog ) ) {
    if ( le<uint16_t>( rsb.s ) == sizeof( HyperLogLog ) / 8 ) {
      this->set_value( MD_HYPERLOGLOG, EKF_KEYSPACE_HLL, rsb.s, rsb.len );
      return;
    }
  }
  this->set_value( MD_STRING, EKF_KEYSPACE_STRING, rsb.s, rsb.len );
}

void
ExecRestore::d_list( const RdbListElem &l ) noexcept
{
  ExecReStrBuf rsb( l.val );
  ExecRestoreCtx<ListData> ctx( *this, this->list );

  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->rpush( rsb.s, rsb.len ) != LIST_FULL )
        return;
    this->list = ctx.realloc( rsb.len + 1, l.cnt + 1 );
  }
}

void
ExecRestore::d_set( const RdbSetMember &s ) noexcept
{
  ExecReStrBuf mem( s.member );
  ExecRestoreCtx<SetData> ctx( *this, this->set );

  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->sadd( mem.s, mem.len ) != SET_FULL )
        return;
    this->set = ctx.realloc( mem.len + 1, s.cnt + 1 );
  }
}

void
ExecRestore::d_zset( const RdbZSetMember &z ) noexcept
{
  ExecReStrBuf mem( z.member ),
               score( z.score );

  if ( this->zset == NULL ) {
    this->is_geo = false;
    if ( score.len >= 18 ) {
      GeoIndx test_h3;
      if ( string_to_uint( score.s, score.len, test_h3 ) == 0 &&
           h3IsValid( test_h3 ) )
        this->is_geo = true;
    }
  }
  if ( ! this->is_geo ) {
    ZScore sc = ZScore::parse_len( score.s, score.len );
    ExecRestoreCtx<ZSetData> ctx( *this, this->zset );

    for (;;) {
      if ( ctx.x != NULL )
        if ( ctx.x->zadd( mem.s, mem.len, sc, 0 ) != ZSET_FULL )
          return;
      this->zset = ctx.realloc( mem.len + 1, z.cnt + 1 );
    }
  }
  else {
    GeoIndx h3;
    ExecRestoreCtx<GeoData> ctx( *this, this->geo );

    if ( string_to_uint( score.s, score.len, h3 ) == 0 ) {
      for (;;) {
        if ( ctx.x != NULL )
          if ( ctx.x->geoadd( mem.s, mem.len, h3 ) != GEO_FULL )
            return;
        this->geo = ctx.realloc( mem.len + 1, z.cnt + 1 );
      }
    }
  }
}

void
ExecRestore::d_hash( const RdbHashEntry &h ) noexcept
{
  ExecReStrBuf fld( h.field ),
               val( h.val );
  ExecRestoreCtx<HashData> ctx( *this, this->hash );

  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->hset( fld.s, fld.len, val.s, val.len ) != HASH_FULL )
        return;
    this->hash = ctx.realloc( fld.len + val.len + 3, h.cnt + 1 );
  }
}

ListData *
ExecRestore::alloc_list( size_t &count,  size_t &ndata )
{
  void * mem;
  size_t sz = ListData::alloc_size( count, ndata );
  this->tmp.alloc( sizeof( ListData ) + sz, &mem );
  ::memset( mem, 0, sizeof( ListData ) + sz );
  void     * p  = &((char *) mem)[ sizeof( ListData ) ];
  ListData * xl = new ( mem ) ListData( p, sz );
  xl->init( count, ndata );
  return xl;
}

void
ExecRestore::d_stream_entry( const RdbStreamEntry &entry ) noexcept
{
  char   id[ 64 ],
         buf[ 24 ];
  size_t idlen,
         count,
         ndata,
         k, d;

  idlen = uint_to_str( entry.id.ms + entry.diff.ms, id );
  id[ idlen++ ] = '-';
  idlen += uint_to_str( entry.id.ser + entry.diff.ser, &id[ idlen ] );

  ndata = idlen + 1;
  count = entry.entry_field_count;
  for ( k = 0; k < count; k++ ) {
    RdbListValue & f = entry.fields[ k ],
                 & v = entry.values[ k ];
    if ( f.data != NULL )
      ndata += f.data_len;
    else
      ndata += int_digits( f.ival );
    if ( v.data != NULL )
      ndata += v.data_len;
    else
      ndata += int_digits( v.ival );
  }
  count = count * 2 + 1; /* field name + value + id */

  ListData * xl = this->alloc_list( count, ndata );
  xl->rpush( id, idlen );
  count = entry.entry_field_count;
  for ( k = 0; k < count; k++ ) {
    RdbListValue & f = entry.fields[ k ],
                 & v = entry.values[ k ];
    if ( f.data != NULL )
      xl->rpush( f.data, f.data_len );
    else {
      d = int_to_str( f.ival, buf );
      xl->rpush( buf, d );
    }
    if ( v.data != NULL )
      xl->rpush( v.data, v.data_len );
    else {
      d = int_to_str( v.ival, buf );
      xl->rpush( buf, d );
    }
  }

  ExecRestoreStream ctx( *this, this->strm );
  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->stream.rpush( xl->listp, xl->size ) != LIST_FULL )
        return;
    this->strm = ctx.realloc( xl->size, entry.items_count );
  }
}

void
ExecRestore::d_stream_info( const RdbStreamInfo &info ) noexcept
{
  this->keyp->state |= EKS_NO_UPDATE;
  this->last_ns = info.last.ms * (uint64_t) 1000000 + info.last.ser;
}

void
ExecRestore::d_stream_group( const RdbGroupInfo &group ) noexcept
{
  char   id[ 64 ];
  size_t idlen,
         count = 2,
         ndata;

  idlen = uint_to_str( group.last.ms, id );
  id[ idlen++ ] = '-';
  idlen += uint_to_str( group.last.ser, &id[ idlen ] );
  ndata = idlen + group.gname_len;

  ListData * xl = this->alloc_list( count, ndata );
  xl->rpush( group.gname, group.gname_len );
  xl->rpush( id, idlen );

  ExecRestoreStream ctx( *this, this->strm );
  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->group.rpush( xl->listp, xl->size ) != LIST_FULL )
        return;
    this->strm = ctx.realloc_group( group.cnt * xl->size, group.cnt );
  }
}

void
ExecRestore::d_stream_pend( const RdbPendInfo &pend ) noexcept
{
  if ( pend.last_delivery != pend.id.ms || pend.delivery_cnt != 1 ) {
    if ( this->pcount == 0 )
      this->tmp.alloc( pend.cnt * sizeof( this->pend[ 0 ] ), &this->pend );
    this->pend[ this->pcount ].id_ms         = pend.id.ms;
    this->pend[ this->pcount ].id_ser        = pend.id.ser;
    this->pend[ this->pcount ].last_delivery = pend.last_delivery;
    this->pend[ this->pcount ].delivery_cnt  = pend.delivery_cnt;
    this->pcount++;
  }
}

void
ExecRestore::d_stream_cons( const RdbConsumerInfo &/*cons*/ ) noexcept
{
}

void
ExecRestore::d_stream_cons_pend( const RdbConsPendInfo &pend ) noexcept
{
  char         id[ 64 ];
  size_t       idlen,
               count = 5,
               ndata,
               glen,
               clen;
  const char * gname,
             * cname;
  uint64_t     ns  = 0;
  uint32_t     cnt = 1;

  idlen = uint_to_str( pend.id.ms, id );
  id[ idlen++ ] = '-';
  idlen += uint_to_str( pend.id.ser, &id[ idlen ] );
  gname  = pend.cons.group.gname;
  glen   = pend.cons.group.gname_len;
  cname  = pend.cons.cname;
  clen   = pend.cons.cname_len;
  ndata  = idlen + glen + clen + 8 + 4;

  ListData * xl = this->alloc_list( count, ndata );

  ns  = pend.id.ms;
  cnt = 1;
  for ( size_t i = 0; i < this->pcount; i++ ) { /* maybe bsearch this */
    if ( pend.id.ms  == this->pend[ i ].id_ms &&
         pend.id.ser == this->pend[ i ].id_ser ) {
      ns  = this->pend[ i ].last_delivery;
      cnt = this->pend[ i ].delivery_cnt;
      break;
    }
  }
  ns *= 1000000;

  xl->rpush( id, idlen );   /* ID */
  xl->rpush( gname, glen ); /* group */
  xl->rpush( cname, clen ); /* consumer */
  xl->rpush( &ns, 8 );      /* last delivered */
  xl->rpush( &cnt, 4 );     /* delivery count */

  ExecRestoreStream ctx( *this, this->strm );
  for (;;) {
    if ( ctx.x != NULL )
      if ( ctx.x->pending.rpush( xl->listp, xl->size ) != LIST_FULL )
        return;
    this->strm = ctx.realloc_pending( pend.cons.group.pending_cnt * xl->size,
                                      pend.cons.group.pending_cnt );
  }
}

static const char *
get_rdb_err_description( RdbErrCode err )
{
  switch ( err ) {
    default:
    case RDB_OK:          return "ok";
    case RDB_EOF_MARK:    return "eof";
    case RDB_ERR_OUTPUT:  return "No output";
    case RDB_ERR_TRUNC:   return "Input truncated";
    case RDB_ERR_VERSION: return "Rdb version too old";
    case RDB_ERR_CRC:     return "Crc does not match";
    case RDB_ERR_TYPE:    return "Error unknown type";
    case RDB_ERR_HDR:     return "Error parsing header";
    case RDB_ERR_LZF:     return "Bad LZF compression";
    case RDB_ERR_NOTSUP:  return "Object type not supported";
    case RDB_ERR_ZLEN:    return "Zip list len mismatch";
  }
}

ExecStatus
RedisExec::exec_restore( EvKeyCtx &ctx ) noexcept
{
  const char * val;
  size_t       val_len;

  /* RESTORE key ttl value [replace] [absttl] [idletime idle] [freq lfu] */
  if ( ! this->msg.get_arg( 3, val, val_len ) )
    return ERR_BAD_ARGS;

  RdbBufptr    bptr( (const uint8_t *) val, val_len );
  RdbDecode    decode;
  ExecRestore  rest( decode, *this, &ctx, val_len );
  RdbErrCode   err;
  int64_t      ttl,
               idletime;
  bool         is_absttl = false;

  if ( ! this->msg.get_arg( 2, ttl ) )
    return ERR_BAD_ARGS;
  else if ( ttl != 0 )
    rest.ttl_ns = (uint64_t) ttl * (uint64_t) 1000000;

  for ( size_t i = 4; i < this->argc; i++ ) {
    switch ( this->msg.match_arg( i, MARG( "replace" ),
                                     MARG( "absttl" ),
                                     MARG( "idletime" ),
                                     MARG( "freq" ), NULL ) ) {
      default: return ERR_BAD_ARGS;
      case 1:  rest.is_replace = true; break;
      case 2:  is_absttl = true; break;
      case 3:
        if ( ! this->msg.get_arg( ++i, idletime ) )
          return ERR_BAD_ARGS;
        if ( idletime != 0 ) {
          ctx.state |= EKS_NO_UPDATE;
          rest.last_ns = this->kctx.ht.hdr.current_stamp -
                         (uint64_t) idletime * (uint64_t) 1000000;
        }
        break;
      case 4:
        if ( ! this->msg.get_arg( ++i, rest.freq ) )
          return ERR_BAD_ARGS;
        break;
    }
  }
  /* adjust ttl to be absolute */
  if ( ! is_absttl || rest.ttl_ns < TEN_YEARS_NS ) {
    if ( rest.ttl_ns != 0 )
      rest.ttl_ns += this->kctx.ht.hdr.current_stamp;
  }
  decode.data_out = &rest;

  if ( (err = decode.decode_hdr( bptr )) == RDB_OK )
    err = decode.decode_body( bptr );

  if ( err == RDB_OK )
    return rest.status;
  const char * s   = get_rdb_err_description( err );
  size_t       len = ::strlen( s );
  char       * buf = this->strm.alloc( len + 9 + 2 );
  ::memcpy( buf, "-ERR RDB-", 9 );
  ::memcpy( &buf[ 9 ], s, len );
  strm.sz += crlf( buf, len + 9 );

  return EXEC_OK;
}

ExecStatus
RedisExec::exec_load( void ) noexcept
{
  ExecStatus status = EXEC_SEND_OK;
  int        fd  = ::open( "dump.rdb", O_RDONLY );
  void     * map = NULL;
  struct stat st;

  ::memset( &st, 0, sizeof( st ) );
  if ( fd < 0 || ::fstat( fd, &st ) != 0 ) {
    perror( "dump.rdb" );
    status = ERR_LOAD;
  }
  if ( status == EXEC_SEND_OK ) {
    map = ::mmap( 0, st.st_size, PROT_READ, MAP_SHARED, fd, 0 );
    if ( map == MAP_FAILED ) {
      perror( "mmap" );
      status = ERR_LOAD;
      map = NULL;
    }
    else {
      ::madvise( map, st.st_size, MADV_SEQUENTIAL );
    }
  }
  if ( map != NULL ) {
    RdbBufptr    bptr( (const uint8_t *) map, st.st_size );
    RdbDecode    decode;
    ExecRestore  rest( decode, *this, NULL, 0 );

    this->cmd_state |= CMD_STATE_LOAD;
    decode.data_out = &rest;
    for (;;) {
      RdbErrCode err = decode.decode_hdr( bptr ); /* find type, len and key */
      if ( err == RDB_OK )
        err = decode.decode_body( bptr );         /* decode the data type */
      if ( err != RDB_OK ) {
        if ( err == RDB_EOF_MARK )             /* 0xff marker found */
          goto success;
        decode.data_out->d_finish( false );
        fprintf( stderr, "%s\n", get_rdb_err_description( err ) );
        status = ERR_LOAD;
        break;
      }
      decode.key_cnt++;
      /* release lzf decompress allocations */
      if ( bptr.alloced_mem != NULL )
        bptr.free_alloced();
      if ( bptr.avail == 0 ) { /* no data left */
    success:;
        decode.data_out->d_finish( true );
        status = rest.status;
        break;
      }
    }
    ::munmap( map, st.st_size );
    this->cmd_state &= ~CMD_STATE_LOAD;
  }
  if ( fd >= 0 )
    ::close( fd );
  return status;
}

