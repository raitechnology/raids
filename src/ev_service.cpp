#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <raids/ev_service.h>

using namespace rai;
using namespace ds;
using namespace kv;

void
EvService::process( bool use_prefetch )
{
  StreamBuf       & strm = *this;
  EvPrefetchQueue * q    = ( use_prefetch ? this->poll.prefetch_queue : NULL );
  size_t            buflen,
                    arg0len;
  const char      * arg0;
  char              upper_cmd[ 32 ];
  RedisMsgStatus    status;
  ExecStatus        err;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 ) {
      this->pop( EV_PROCESS );
      break;
    }
    if ( strm.idx + strm.vlen / 4 >= strm.vlen ) {
      if ( ! this->try_write() || strm.idx + 8 >= strm.vlen )
        break;
    }
    status = this->msg.unpack( &this->recv[ this->off ], buflen, strm.tmp );
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
    this->off += buflen;

    arg0 = this->msg.command( arg0len, this->argc );
    /* max command len is 17 (GEORADIUSBYMEMBER) */
    err = ( arg0len < 32 ) ? EXEC_OK : EXEC_BAD_CMD;

    if ( err == EXEC_OK ) {
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
      if ( (err = this->exec( this, q )) == EXEC_OK && strm.alloc_fail )
        err = EXEC_ALLOC_FAIL;
    }
    if ( err == EXEC_SETUP_OK ) {
      if ( q != NULL )
        return;
      if ( ! this->exec_key_continue( *this->key ) ) {
        while ( ! this->exec_key_continue( *this->keys[ this->key_done ] ) )
          ;
      }
    }
    else if ( err == EXEC_QUIT ) {
      this->poll.quit++;
    }
    else {
      this->send_err( err );
    }
  }
  if ( strm.wr_pending + strm.sz > 0 )
    this->push( EV_WRITE );
}

void
EvService::debug( void )
{
  const char *name[] = { 0, "wait", "read", "process", "write", "close" };
  struct sockaddr_storage addr;
  socklen_t addrlen;
  char buf[ 128 ], svc[ 32 ];
  EvSocket *s, *next;
  int i;
  for ( i = 0; i < (int) EvPoll::PREFETCH_SIZE; i++ ) {
    if ( this->poll.prefetch_cnt[ i ] != 0 )
      printf( "[%d]: %lu\n", i, this->poll.prefetch_cnt[ i ] );
  }
  for ( i = 1; i <= 5; i++ ) {
    printf( "%s: ", name[ i ] );
    for ( s = this->poll.queue[ i ].hd; s != NULL; s = next ) {
      next = s->next[ i ];
      if ( s->type == EV_SERVICE_SOCK ) {
	addrlen = sizeof( addr );
	getpeername( s->fd, (struct sockaddr*) &addr, &addrlen );
	getnameinfo( (struct sockaddr*) &addr, addrlen, buf, sizeof( buf ),
                     svc, sizeof( svc ), NI_NUMERICHOST | NI_NUMERICSERV );
      }
      else {
        buf[ 0 ] = 'L'; buf[ 1 ] = '\0';
        svc[ 0 ] = 0;
      }
      printf( "%d/%s:%s ", s->fd, buf, svc );
    } 
    printf( "\n" );
  }
  for ( i = 1; i <= 5; i++ ) {
    for ( s = this->poll.queue[ i ].hd; s != NULL; s = next ) {
      next = s->next[ i ];
      if ( s->type == EV_SERVICE_SOCK ) {
	if ( ((EvService *) s)->off != ((EvService *) s)->len ) {
	  printf( "%p: (%d) has buf(%u)\n", s, s->fd,
		  ((EvService *) s)->len - ((EvService *) s)->off );
	}
	if ( ((EvService *) s)->wr_pending + ((EvService *) s)->sz != 0 ) {
	  printf( "%p: (%d) has pend(%lu)\n", s, s->fd,
	         ((EvService *) s)->wr_pending + ((EvService *) s)->sz );
	}
      }
    } 
  }
  if ( this->poll.prefetch_queue->is_empty() )
    printf( "prefetch empty\n" );
  else
    printf( "prefetch count %lu\n",
	    this->poll.prefetch_queue->count() );
}

ExecStatus
RedisExec::exec_debug( EvService *own )
{
  printf( "debug\n" );
  own->debug();
  return EXEC_SEND_OK;
}

