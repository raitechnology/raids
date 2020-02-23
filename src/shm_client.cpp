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
EvShm::open( const char *mn ) noexcept
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
EvShm::print( void ) noexcept
{
  fputs( print_map_geom( this->map, this->ctx_id ), stdout );
  fflush( stdout );
}

EvShm::~EvShm() noexcept
{
  if ( this->map != NULL )
    this->close();
}

void
EvShm::close( void ) noexcept
{
  if ( this->ctx_id != MAX_CTX_ID ) {
    map->detach_ctx( ctx_id );
    this->ctx_id = MAX_CTX_ID;
  }
  delete this->map;
  this->map = NULL;
}

EvShmClient::~EvShmClient() noexcept
{
}

void
EvShmClient::process_shutdown( void ) noexcept
{
  this->exec->rem_all_sub();
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

int
EvShmClient::init_exec( void ) noexcept
{
  void * e = aligned_malloc( sizeof( RedisExec ) );
  if ( e == NULL )
    return -1;
  if ( ::pipe2( this->pfd, O_NONBLOCK ) < 0 )
    return -1;
  this->PeerData::init_peer( this->pfd[ 0 ], NULL, "shm" );
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, *this,
                                    this->poll.sub_route, *this );
  this->exec->sub_id = this->fd;
  this->poll.add_sock( this );
  return 0;
}

bool
EvShmClient::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  int status = this->exec->do_pub( pub, cm );
  if ( ( status & RPUB_FORWARD_MSG ) != 0 )
    this->stream_to_msg();
  if ( ( status & RPUB_CONTINUE_MSG ) != 0 )
    this->exec->push_continue_list( cm );
  return true;
}

bool
EvShmClient::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  return this->exec->do_hash_to_sub( h, key, keylen );
}

void
EvShmClient::send_data( char *buf,  size_t size ) noexcept
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
      this->exec->send_status( status, KEY_OK );
      break;
    case EXEC_QUIT:
    case EXEC_DEBUG:
      break;
  }
  this->stream_to_msg();
}

void
EvShmClient::stream_to_msg( void ) noexcept
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
EvShmSvc::~EvShmSvc() noexcept {}
int EvShmSvc::init_poll( void ) noexcept {
  int status;
  this->PeerData::init_peer( 0, NULL, "shm_svc" );
  if ( (status = this->poll.add_sock( this )) == 0 )
    return 0;
  this->fd = -1;
  return status;
}
bool EvShmSvc::timer_expire( uint64_t,  uint64_t ) noexcept { return false; }
void EvShmSvc::read( void ) noexcept {}
void EvShmSvc::write( void ) noexcept {}
bool EvShmSvc::on_msg( EvPublish & ) noexcept { return false; }
bool EvShmSvc::hash_to_sub( uint32_t,  char *,  size_t & ) noexcept { return false; }
void EvShmSvc::process( void ) noexcept {}
void EvShmSvc::process_shutdown( void ) noexcept {}
void EvShmSvc::process_close( void ) noexcept {}
void EvShmSvc::release( void ) noexcept {}
