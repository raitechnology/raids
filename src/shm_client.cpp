#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <raids/ev_client.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;
using namespace kv;

int
EvShmClient::open( const char *mn )
{
  HashTabGeom geom;
  this->map = HashTab::attach_map( mn, 0, geom );
  if ( this->map != NULL ) {
    this->ctx_id = map->attach_ctx( ::getpid(), 0 );
    return 0;
  }
  return -1;
}

void
EvShmClient::print( void )
{
  fputs( print_map_geom( this->map, this->ctx_id ), stdout );
  fflush( stdout );
}

EvShmClient::~EvShmClient()
{
  if ( this->map != NULL )
    this->close();
}

void
EvShmClient::close( void )
{
  if ( this->ctx_id != MAX_CTX_ID ) {
    map->detach_ctx( ctx_id );
    this->ctx_id = MAX_CTX_ID;
  }
  delete this->map;
  this->map = NULL;
}

int
EvShmClient::init_exec( EvPoll &p )
{
  void * e = ::malloc( sizeof( RedisExec ) );
  if ( e == NULL )
    return -1;
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, *this,
                                    p.sub_route, p.single_thread );
  return 0;
}

void
EvShmClient::send_msg( RedisMsg &msg )
{
  ExecStatus status;

  this->exec->msg.ref( msg );

  if ( (status = this->exec->exec( NULL, NULL )) == EXEC_OK )
    if ( this->alloc_fail )
      status = ERR_ALLOC_FAIL;
  switch ( status ) {
    case EXEC_SETUP_OK:
      this->exec->exec_run_to_completion();
      if ( ! this->alloc_fail )
        break;
      status = ERR_ALLOC_FAIL;
      /* fall through */
    default:
      this->exec->send_err( status );
      break;
    case EXEC_QUIT:
    case EXEC_DEBUG:
      break;
  }
  if ( this->sz > 0 )
    this->flush();
  if ( this->idx >= 1 ) {
    void * buf = this->iov[ 0 ].iov_base;
    size_t len = this->iov[ 0 ].iov_len;
    RedisMsg out;
    if ( this->idx > 1 ) {
      size_t i;
      for ( i = 1; i < this->idx; i++ )
        len += this->iov[ i ].iov_len;
      buf = this->alloc_temp( len );
      len = 0;
      if ( buf != NULL ) {
        for ( i = 0; i < this->idx; i++ ) {
          size_t add = this->iov[ i ].iov_len;
          ::memcpy( &((char *) buf)[ len ], this->iov[ i ].iov_base, add );
          len += add;
        }
      }
    }
    if ( len > 0 && out.unpack( buf, len, this->tmp ) == REDIS_MSG_OK )
      this->cb.on_msg( out );
  }
  this->reset();
}

