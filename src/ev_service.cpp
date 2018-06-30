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
  size_t            buflen;
  RedisMsgStatus    mstatus;
  ExecStatus        status;

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
    mstatus = this->msg.unpack( &this->recv[ this->off ], buflen, strm.tmp );
    if ( mstatus != REDIS_MSG_OK ) {
      if ( mstatus != REDIS_MSG_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, redis_msg_status_string( mstatus ), buflen );
        this->off = this->len;
        break;
      }
      if ( ! this->try_read() )
        break;
      continue;
    }
    this->off += buflen;

    if ( (status = this->exec( this, q )) == EXEC_OK )
      if ( strm.alloc_fail )
        status = EXEC_ALLOC_FAIL;
    if ( status == EXEC_SETUP_OK ) {
      if ( q != NULL )
        return;
      if ( this->key_cnt == 1 ) { /* only one key */
        while ( this->key->status == EXEC_CONTINUE ||
                this->key->status == EXEC_DEPENDS )
          this->exec_key_continue( *this->key );
      }
      else {
        /* cycle through keys */
        uint32_t j = 0;
        for ( uint32_t i = 0; ; ) {
          if ( this->keys[ i ]->status == EXEC_CONTINUE ||
               this->keys[ i ]->status == EXEC_DEPENDS ) {
            if ( this->exec_key_continue( *this->keys[ i ] ) == EXEC_SUCCESS )
              break;
            j = 0;
          }
          else if ( ++j == this->key_cnt )
            break;
          if ( ++i == this->key_cnt )
            i = 0;
        }
      }
    }
    else if ( status == EXEC_QUIT ) {
      this->poll.quit++;
    }
    else if ( status == EXEC_DEBUG ) {
      this->debug();
    }
    else {
      this->send_err( status );
    }
  }
  if ( strm.wr_pending + strm.sz > 0 )
    this->push( EV_WRITE );
}

void
EvService::release( void )
{
  this->RedisExec::release();
  this->EvConnection::release();
  this->push_free_list();
}

void
EvService::push_free_list( void )
{
  if ( this->state != 0 )
    this->popall();
  this->next[ 0 ] = this->poll.free_svc;
  this->poll.free_svc = this;
}

void
EvService::pop_free_list( void )
{
  this->poll.free_svc = this->next[ 0 ];
  this->next[ 0 ] = NULL;
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
  for ( i = EV_WAIT; i < EV_MAX; i++ ) {
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
  for ( i = EV_WAIT; i < EV_MAX; i++ ) {
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

