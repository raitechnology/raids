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
EvShm::open( const char *mn )
{
  HashTabGeom geom;
  this->map = HashTab::attach_map( mn, 0, geom );
  if ( this->map != NULL ) {
    this->ctx_id = map->attach_ctx( ::getpid(), 0, 254 );
    return 0;
  }
  return -1;
}

void
EvShm::print( void )
{
  fputs( print_map_geom( this->map, this->ctx_id ), stdout );
  fflush( stdout );
}

EvShm::~EvShm()
{
  if ( this->map != NULL )
    this->close();
}

void
EvShm::close( void )
{
  if ( this->ctx_id != MAX_CTX_ID ) {
    map->detach_ctx( ctx_id );
    this->ctx_id = MAX_CTX_ID;
  }
  delete this->map;
  this->map = NULL;
}

EvShmClient::~EvShmClient()
{
}

void
EvShmClient::process_shutdown( void )
{
  this->exec->rem_all_sub();
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

int
EvShmClient::init_exec( void )
{
  void * e = aligned_malloc( sizeof( RedisExec ) );
  if ( e == NULL )
    return -1;
  if ( ::pipe2( this->pfd, O_NONBLOCK ) < 0 )
    return -1;
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, *this,
                                    this->poll.sub_route );
  this->fd = this->pfd[ 0 ];
  this->exec->sub_id = this->fd;
  this->poll.add_sock( this );
  return 0;
}

bool
EvShmClient::on_msg( EvPublish &pub )
{
  if ( this->exec->do_pub( pub ) )
    this->stream_to_msg();
  return true;
}

bool
EvShmClient::hash_to_sub( uint32_t h,  char *key,  size_t &keylen )
{
  return this->exec->do_hash_to_sub( h, key, keylen );
}

void
EvShmClient::send_data( char *buf,  size_t size )
{
  ExecStatus status;

  if ( this->exec->msg.unpack( buf, size, this->tmp ) != REDIS_MSG_OK )
    return;
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
  this->stream_to_msg();
}

void
EvShmClient::stream_to_msg( void )
{
  if ( this->sz > 0 )
    this->flush();
  if ( this->idx >= 1 ) {
    void * buf = this->iov[ 0 ].iov_base;
    size_t len = this->iov[ 0 ].iov_len;
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
    for ( size_t off = 0; ; ) {
      size_t buflen = len - off;
      if ( buflen == 0 )
        break;
      if ( this->cb.on_data( &((char *) buf)[ off ], buflen ) )
        off += buflen;
      else
        break;
    }
  }
  this->reset();
}

/* EvShmSvc virtual functions */
EvShmSvc::~EvShmSvc() {}
int EvShmSvc::init_poll( void ) {
  int status;
  this->fd = 0;
  if ( (status = this->poll.add_sock( this )) == 0 )
    return 0;
  this->fd = -1;
  return status;
}
bool   EvShmSvc::timer_expire( uint64_t ) { return false; }
bool   EvShmSvc::read( void ) { return false; }
size_t EvShmSvc::write( void ) { return 0; }
bool   EvShmSvc::on_msg( EvPublish & ) { return false; }
bool   EvShmSvc::hash_to_sub( uint32_t,  char *,  size_t & ) { return false; }
void   EvShmSvc::process( bool ) {}
void   EvShmSvc::process_shutdown( void ) {}
void   EvShmSvc::process_close( void ) {}
void   EvShmSvc::release( void ) {}

