#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <raids/ev_service.h>

using namespace rai;
using namespace ds;
using namespace kv;

void
EvService::process( void )
{
  StreamBuf &strm = *this;
  for (;;) {
    size_t buflen = this->len - this->off;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    if ( strm.idx + strm.vlen / 4 >= strm.vlen ) {
      if ( ! this->try_write() || strm.idx + 8 >= strm.vlen )
        break;
    }
    RedisMsgStatus status =
      this->msg.unpack( &this->recv[ this->off ], buflen, strm.out );
    if ( status != REDIS_MSG_OK ) {
      if ( status != REDIS_MSG_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 status, redis_msg_status_string( status ), buflen );
        this->off = this->len;
        break;
      }
      if ( ! this->try_read() )
        break;
      continue;
    }
    size_t arg0len;
    const char * arg0 = this->msg.command( arg0len, this->argc );
    /* max command len is 17 (GEORADIUSBYMEMBER) */
    ExecStatus err = ( arg0len < 32 ) ? EXEC_OK : EXEC_BAD_CMD;

    if ( err == EXEC_OK ) {
      char upper_cmd[ 32 ];
      str_to_upper( arg0, upper_cmd, arg0len );
      if ( (this->cmd = get_redis_cmd( upper_cmd, arg0len )) == NO_CMD )
        err = EXEC_BAD_CMD;
      else {
        get_cmd_arity( this->cmd, this->arity, this->first, this->last,
                       this->step );
        if ( this->arity > 0 ) {
          if ( (size_t) this->arity != this->argc )
            err = EXEC_BAD_ARGS;
        }
        else if ( (size_t) -this->arity > this->argc )
          err = EXEC_BAD_ARGS;
      }
    }
    if ( err == EXEC_OK ) {
      this->flags = get_cmd_flag_mask( this->cmd );
      if ( (err = this->exec()) == EXEC_OK && strm.alloc_fail )
        err = EXEC_ALLOC_FAIL;
    }
    switch ( err ) {
      case EXEC_OK:         break;
      case EXEC_SEND_OK:    this->send_ok(); break;
      case EXEC_KV_STATUS:  this->send_err_kv( this->kstatus ); break;
      case EXEC_MSG_STATUS: this->send_err_msg( this->mstatus ); break;
      case EXEC_BAD_ARGS:   this->send_err_bad_args(); break;
      case EXEC_BAD_CMD:    this->send_err_bad_cmd(); break;
      case EXEC_QUIT:       this->send_ok(); this->poll.quit++; break;
      case EXEC_ALLOC_FAIL: this->send_err_alloc_fail(); break;
    }
    this->off += buflen;
  }
  if ( strm.wr_pending + strm.sz > 0 )
    this->push( EV_WRITE );
}

void
EvService::send_ok( void )
{
  static char ok[] = "+OK\r\n";
  this->StreamBuf::append( ok, 5 );
}

void
EvService::send_err_bad_args( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  StreamBuf  & strm = *this;
  void       * buf  = strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz,
              "-ERR wrong number of arguments for '%.*s' command\r\n",
              (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

void
EvService::send_err_kv( int kstatus )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 256;
  StreamBuf  & strm = *this;
  void       * buf  = strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': KeyCtx %d/%s %s\r\n",
                     (int) arg0len, arg0,
                     kstatus, kv_key_status_string( (KeyStatus) kstatus ),
                     kv_key_status_description( (KeyStatus) kstatus ) );
    strm.sz += bsz;
  }
}

void
EvService::send_err_msg( int mstatus )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 256;
  StreamBuf  & strm = *this;
  void       * buf  = strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': RedisMsg %d/%s %s\r\n",
                   (int) arg0len, arg0,
                   mstatus, redis_msg_status_string( (RedisMsgStatus) mstatus ),
                   redis_msg_status_description( (RedisMsgStatus) mstatus ) );
    strm.sz += bsz;
  }
}

void
EvService::send_err_bad_cmd( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  StreamBuf  & strm = *this;
  void       * buf  = strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR unknown command: '%.*s'\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
  {
    char tmpbuf[ 1024 ];
    size_t tmpsz = sizeof( tmpbuf );
    if ( this->msg.to_json( tmpbuf, tmpsz ) == REDIS_MSG_OK )
      fprintf( stderr, "Bad command: %s\n", tmpbuf );
  }
}

void
EvService::send_err_alloc_fail( void )
{
  size_t       arg0len;
  const char * arg0 = this->msg.command( arg0len );
  size_t       bsz  = 64 + 24;
  StreamBuf  & strm = *this;
  void       * buf  = strm.alloc( bsz );

  if ( buf != NULL ) {
    arg0len = ( arg0len < 24 ? arg0len : 24 );
    bsz = ::snprintf( (char *) buf, bsz, "-ERR '%.*s': allocation failure\r\n",
                     (int) arg0len, arg0 );
    strm.sz += bsz;
  }
}

