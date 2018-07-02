#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <raids/ev_client.h>

using namespace rai;
using namespace ds;

void
EvClient::process( void )
{
  for (;;) {
    size_t buflen = this->len - this->off;
    RedisMsgStatus status;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    if ( this->idx + this->vlen / 4 >= this->vlen ) {
      if ( ! this->try_write() || this->idx + 8 >= this->vlen )
        break;
    }
    switch ( this->recv[ this->off ] ) {
      default:
      case RedisMsg::SIMPLE_STRING: /* + */
      case RedisMsg::ERROR_STRING:  /* - */
      case RedisMsg::INTEGER_VALUE: /* : */
      case RedisMsg::BULK_STRING:   /* $ */
      case RedisMsg::BULK_ARRAY:    /* * */
	status = this->msg.unpack( &this->recv[ this->off ], buflen,
                                   this->tmp );
        break;
      case ' ':
      case '\t':
      case '\r':
      case '\n':
        buflen = 1;
        goto skip_char;
      case '"':
      case '\'':
      case '[':
      case '0': case '1': case '2': case '3': case '4':
      case '5': case '6': case '7': case '8': case '9':
        status = this->msg.unpack_json( &this->recv[ this->off ], buflen,
                                        this->tmp );
        break;
    }
    if ( status != REDIS_MSG_OK ) {
      if ( status != REDIS_MSG_PARTIAL ) {
        this->cb.onErr( &this->recv[ this->off ], buflen, status );
        this->off = this->len;
        break;
      }
      if ( ! this->try_read() )
        break;
      continue;
    }
    this->cb.onMsg( this->msg );
  skip_char:;
    this->off += buflen;
  }
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
}

void
EvClient::process_close( void )
{
  this->cb.onClose();
}

void
EvCallback::onMsg( RedisMsg &msg )
{
  char buf[ 64 * 1024 ];
  size_t sz;
  if ( (sz = msg.to_almost_json_size()) < sizeof( buf ) ) {
    if ( sz > 0 )
      msg.to_almost_json( buf );
    fprintf( stderr, "onMsg: %*s", (int) sz, buf );
  }
}

void
EvCallback::onErr( char *buf,  size_t buflen,  RedisMsgStatus status )
{
  fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
	   status, redis_msg_status_string( status ), buflen );
}

void
EvCallback::onClose( void )
{
  fprintf( stderr, "closed\n" );
}

