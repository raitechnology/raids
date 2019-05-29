#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <raids/ev_client.h>
#include <linecook/linecook.h>
#include <linecook/ttycook.h>

using namespace rai;
using namespace ds;

void
EvClient::send_msg( RedisMsg & )
{
}

void
EvNetClient::send_msg( RedisMsg &msg )
{
  void *tmp = this->tmp.alloc( msg.pack_size() );
  if ( tmp != NULL ) {
    this->append_iov( tmp, msg.pack( tmp ) );
    this->idle_push( EV_WRITE );
  }
}

RedisMsgStatus
EvNetClient::process_msg( char *buf,  size_t &buflen )
{
  switch ( buf[ 0 ] ) {
    default:
    case RedisMsg::SIMPLE_STRING: /* + */
    case RedisMsg::ERROR_STRING:  /* - */
    case RedisMsg::INTEGER_VALUE: /* : */
    case RedisMsg::BULK_STRING:   /* $ */
    case RedisMsg::BULK_ARRAY:    /* * */
      return this->msg.unpack( buf, buflen, this->tmp );
    case ' ':
    case '\t':
    case '\r':
    case '\n':
      buflen = 1;
      return REDIS_MSG_PARTIAL;
    case '"':
    case '\'':
    case '[':
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
      return this->msg.unpack_json( buf, buflen, this->tmp );
  }
}

void
EvNetClient::process( void )
{
  for (;;) {
    char * buf    = &this->recv[ this->off ];
    size_t buflen = this->len - this->off;
    RedisMsgStatus status;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    if ( this->idx + this->vlen / 4 >= this->vlen ) {
      if ( this->try_write() == 0 || this->idx + 8 >= this->vlen )
        break;
    }
    status = this->process_msg( buf, buflen );
    if ( status != REDIS_MSG_OK ) {
      if ( status != REDIS_MSG_PARTIAL ) {
        this->cb.on_err( &this->recv[ this->off ], buflen, status );
        this->off = this->len;
        break;
      }
    }
    else {
      this->cb.on_msg( this->msg );
    }
    this->off += buflen;
  }
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
}

void
EvNetClient::process_close( void )
{
  this->cb.on_close();
}

void
EvCallback::on_msg( RedisMsg &msg )
{
  char buf[ 64 * 1024 ];
  size_t sz;
  if ( (sz = msg.to_almost_json_size()) < sizeof( buf ) ) {
    if ( sz > 0 )
      msg.to_almost_json( buf );
    fprintf( stderr, "on_msg: %*s", (int) sz, buf );
  }
}

void
EvCallback::on_err( char *,  size_t buflen,  RedisMsgStatus status )
{
  fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
	   status, redis_msg_status_string( status ), buflen );
}

void
EvCallback::on_close( void )
{
  fprintf( stderr, "closed\n" );
}

void
EvTerminal::process( void )
{
  size_t buflen = this->len - this->off;
  size_t msgcnt = 0;
  RedisMsgStatus status;
  this->term.tty_input( &this->recv[ this->off ], buflen );
  this->off = this->len;

  for (;;) {
    buflen = this->term.line_len - this->term.line_off;
    if ( buflen == 0 )
      break;
    char * buf = &this->term.line_buf[ this->term.line_off ];
    status = this->process_msg( buf, buflen );
    if ( status != REDIS_MSG_OK ) {
      if ( status != REDIS_MSG_PARTIAL ) {
        this->cb.on_err( buf, buflen, status );
        this->term.line_off = this->term.line_len;
        msgcnt++;
      }
    }
    else {
      this->cb.on_msg( this->msg );
      msgcnt++;
    }
    this->term.line_off += buflen;
  }
  if ( msgcnt > 0 )
    this->term.tty_prompt();
  this->flush_out();

  if ( this->line_len > 0 ) {
    status = this->process_msg( this->line, this->line_len );
    if ( status == REDIS_MSG_OK )
      this->cb.on_msg( this->msg );
    else if ( status != REDIS_MSG_PARTIAL )
      this->cb.on_err( this->line, this->line_len, status );
    ::free( this->line );
    this->line = NULL;
    this->line_len = 0;
  }
  this->pop( EV_PROCESS );
}

void
EvTerminal::process_line( const char *s )
{
  size_t slen = ::strlen( s );
  this->line = (char *) ::realloc( this->line, this->line_len + slen + 1 );
  if ( this->line != NULL ) {
    ::memcpy( &this->line[ this->line_len ], s, slen );
    this->line_len += slen;
  }
  this->idle_push( EV_PROCESS );
}

void
EvTerminal::flush_out( void )
{
  for ( size_t i = 0;;) {
    if ( i == this->term.out_len ) {
      this->term.tty_out_reset();
      return;
    }
    int n = ::write( STDOUT_FILENO, &this->term.out_buf[ i ],
                     this->term.out_len - i );
    if ( n < 0 ) {
      if ( errno != EAGAIN && errno != EINTR ) {
        this->cb.on_close();
        return;
      }
    }
    else {
      i += (size_t) n;
    }
  }
}

int
EvTerminal::start( void )
{
  this->fd = STDIN_FILENO;
  lc_tty_set_locale();
  this->term.tty_init();
  lc_tty_init_fd( this->term.tty, STDIN_FILENO, STDOUT_FILENO );
  lc_tty_init_geom( this->term.tty );     /* try to determine lines/cols */
  lc_tty_init_sigwinch( this->term.tty ); /* install sigwinch handler */
  this->term.tty_prompt();
  this->flush_out();
  return this->poll.add_sock( this );
}

void
EvTerminal::finish( void )
{
  if ( this->term.tty != NULL ) {
    lc_tty_clear_line( this->term.tty );
    this->flush_out();
    lc_tty_normal_mode( this->term.tty );
  }
  this->term.tty_release();
}

void
EvTerminal::printf( const char *fmt, ... )
{
  va_list args;
  lc_tty_clear_line( this->term.tty );
  this->flush_out();
  lc_tty_normal_mode( this->term.tty );
  va_start( args, fmt );
  vprintf( fmt, args );
  va_end( args );
  fflush( stdout );
  this->term.tty_prompt();
  this->flush_out();
}

