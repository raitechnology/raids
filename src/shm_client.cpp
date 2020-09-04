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
EvShm::open( const char *map_name,  uint8_t db_num ) noexcept
{
  HashTabGeom geom;
  this->map = HashTab::attach_map( map_name, 0, geom );
  if ( this->map != NULL )
    return this->attach( db_num );
  return -1;
}

int
EvShm::create( const char *map_name,  kv_geom_t *geom,  int map_mode,
               uint8_t db_num ) noexcept
{
  kv_geom_t default_geom;
  if ( geom == NULL ) {
    ::memset( &default_geom, 0, sizeof( default_geom ) );
    geom = &default_geom;
  }
  if ( geom->map_size == 0 )
    geom->map_size = (uint64_t) 1024*1024*1024;
  if ( geom->hash_value_ratio <= 0.0 || geom->hash_value_ratio > 1.0 )
    geom->hash_value_ratio = 0.25;
  if ( geom->hash_value_ratio < 1.0 ) {
    uint64_t value_space = (uint64_t) ( (double) geom->map_size *
                                        ( 1.0 - geom->hash_value_ratio ) );
    if ( geom->max_value_size == 0 || geom->max_value_size > value_space / 3 ) {
      const uint64_t sz = 256 * 1024 * 1024;
      uint64_t d  = 8; /* 8 = 512MB/64MB, 8 = 1G/128MB, 16 = 2G/256G */
      geom->max_value_size = value_space / d;
      while ( d < 128 && geom->max_value_size / 2 > sz ) {
        geom->max_value_size /= 2;
        d *= 2;
      }
    }
  }
  geom->hash_entry_size = 64;
  geom->cuckoo_buckets  = 2;
  geom->cuckoo_arity    = 4;
  if ( map_mode == 0 )
    map_mode = 0660;
  this->map = HashTab::create_map( map_name, 0, *geom, map_mode );
  if ( this->map != NULL )
    return this->attach( db_num );
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
  /* should explicity close instead */
  /*if ( this->map != NULL )
    this->close();*/
}

int
EvShm::attach( uint8_t db_num ) noexcept
{
  this->ctx_id = this->map->attach_ctx( ::gettid() );
  if ( this->ctx_id != MAX_CTX_ID ) {
    this->dbx_id = this->map->attach_db( this->ctx_id, db_num );
    return 0;
  }
  return -1;
}

void
EvShm::detach( void ) noexcept
{
  if ( this->ctx_id != MAX_CTX_ID ) {
    this->map->detach_ctx( this->ctx_id );
    this->ctx_id = MAX_CTX_ID;
  }
}

void
EvShm::close( void ) noexcept
{
  this->detach();
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
  this->PeerData::init_ctx( this->pfd[ 0 ], this->ctx_id, "shm_client" );
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, this->dbx_id,
                                    *this, this->poll.sub_route, *this );
  this->exec->setup_ids( this->fd, (uint64_t) EV_SHM_SOCK << 56 );
  this->poll.add_sock( this );
  return 0;
}

bool
EvShmClient::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  int status = this->exec->do_pub( pub, cm );
  if ( ( status & RPUB_FORWARD_MSG ) != 0 )
    this->data_callback();
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

  if ( this->exec->msg.unpack( buf, size, this->tmp ) != DS_MSG_STATUS_OK )
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
  this->data_callback();
}

void
EvShmClient::data_callback( void ) noexcept
{
  if ( this->concat_iov() ) {
    void * buf = this->iov[ 0 ].iov_base;
    size_t len = this->iov[ 0 ].iov_len;
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
