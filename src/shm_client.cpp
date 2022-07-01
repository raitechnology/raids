#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <raids/ev_client.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;
using namespace kv;

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
  int status, pfd = this->poll.get_null_fd();
  this->PeerData::init_ctx( pfd, -1, this->ctx_id, "shm_client" );
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, this->dbx_id,
                                    *this, this->poll.sub_route, *this,
                                    this->poll.timer );
  this->exec->setup_ids( pfd, (uint64_t) this->sock_type << 56 );
  if ( (status = this->poll.add_sock( this )) == 0 )
    return 0;
  return status;
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

uint8_t
EvShmClient::is_subscribed( const NotifySub &sub ) noexcept
{
  return this->exec->test_subscribed( sub );
}

uint8_t
EvShmClient::is_psubscribed( const NotifyPattern &pat ) noexcept
{
  return this->exec->test_psubscribed( pat );
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

void EvShmClient::write( void ) noexcept {}
void EvShmClient::read( void ) noexcept {}
void EvShmClient::process( void ) noexcept {}
void EvShmClient::release( void ) noexcept { this->StreamBuf::reset(); }

/* EvShmSvc virtual functions */
EvShmSvc::~EvShmSvc() noexcept {}
int EvShmSvc::init_poll( void ) noexcept {
  int status, pfd = this->poll.get_null_fd();
  this->PeerData::init_peer( pfd, -1, NULL, "shm_svc" );
  if ( (status = this->poll.add_sock( this )) == 0 )
    return 0;
  return status;
}
void EvShmSvc::write( void ) noexcept {}
void EvShmSvc::read( void ) noexcept {}
void EvShmSvc::process( void ) noexcept {}
void EvShmSvc::release( void ) noexcept {}
bool EvShmSvc::timer_expire( uint64_t,  uint64_t ) noexcept { return false; }
bool EvShmSvc::hash_to_sub( uint32_t,  char *,  size_t & ) noexcept { return false; }
bool EvShmSvc::on_msg( EvPublish & ) noexcept { return false; }
void EvShmSvc::key_prefetch( EvKeyCtx & ) noexcept {}
int  EvShmSvc::key_continue( EvKeyCtx & ) noexcept { return 0; }
void EvShmSvc::process_shutdown( void ) noexcept {}
void EvShmSvc::process_close( void ) noexcept { this->EvSocket::process_close(); }
