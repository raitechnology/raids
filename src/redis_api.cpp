#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <raids/redis_api.h>
#include <raids/ev_client.h>
#include <raids/redis_exec.h>
#include <raikv/kv_pubsub.h>

using namespace rai;
using namespace ds;
using namespace kv;

extern "C" {
  struct ds_s {
    uint64_t msg_cnt;
  };
}

namespace {
struct ds_internal : public ds_s, public EvShmApi {
  void * operator new( size_t, void *ptr ) { return ptr; }

  ds_internal( EvPoll &p ) : EvShmApi( p ) {
    this->msg_cnt = 0;
  }
};
}

extern "C" {

int
ds_create( ds_t **h,  const char *map_name,  uint8_t db_num,
           int use_busy_poll,  kv_geom_t *geom,  int map_mode )
{
  size_t        sz   = align<size_t>( sizeof( ds_internal ), 64 );
  void        * m    = aligned_malloc( sz + sizeof( EvPoll ) );
  EvPoll      * poll = new ( &((char *) m)[ sz ] ) EvPoll();
  ds_internal * ds;
  int           status;

  *h = NULL;
  if ( poll->init( 5, false ) != 0 )
    return -1;
  ds = new ( m ) ds_internal( *poll );
  status = ds->create( map_name, geom, map_mode, db_num );
  if ( status == 0 ) {
    if ( status == 0 )
      status = poll->init_shm( *ds );
    status = ds->init_exec();
    if ( status == 0 ) {
      /*poll->pubsub->flags &= ~KV_DO_NOTIFY;*/
      while ( ds_dispatch( ds, 0 ) != 0 )
        ;
    }
    if ( use_busy_poll ) {
      poll->pubsub->idle_push( EV_BUSY_POLL );
      poll->pubsub->flags &= ~KV_DO_NOTIFY;
    }
  }
  if ( status != 0 ) {
    ds_close( ds );
    return -1;
  }
  *h = ds;
  
  return 0;
}

int
ds_open( ds_t **h,  const char *map_name,  uint8_t db_num,
         int use_busy_poll )
{
  size_t        sz   = align<size_t>( sizeof( ds_internal ), 64 );
  void        * m    = aligned_malloc( sz + sizeof( EvPoll ) );
  EvPoll      * poll = new ( &((char *) m)[ sz ] ) EvPoll();
  ds_internal * ds;
  int           status;

  *h = NULL;
  poll->init( 5, false );
  ds = new ( m ) ds_internal( *poll );
  if ( map_name == NULL )
    status = ds->open();
  else
    status = ds->open( map_name, db_num );
  if ( status == 0 ) {
    if ( status == 0 )
      status = poll->init_shm( *ds );
    status = ds->init_exec();
    if ( status == 0 ) {
      /*poll->pubsub->flags &= ~KV_DO_NOTIFY;*/
      while ( ds_dispatch( ds, 0 ) != 0 )
        ;
    }
    if ( use_busy_poll ) {
      poll->pubsub->idle_push( EV_BUSY_POLL );
      poll->pubsub->flags &= ~KV_DO_NOTIFY;
    }
  }
  if ( status != 0 ) {
    ds_close( ds );
    return -1;
  }
  *h = ds;
  
  return 0;
}

int
ds_close( ds_t *h )
{
  ds_internal & ds = *(ds_internal *) h;
  ds.poll.quit++;
  if ( ds.poll.pubsub != NULL )
    ds.poll.pubsub->print_backlog();
  for (;;) {
    if ( ds.poll.quit >= 5 )
      break;
    int idle = ds.poll.dispatch();
    ds.poll.wait( idle == EvPoll::DISPATCH_IDLE ? 10 : 0 );
  }
  ds.close();
  ::free( &ds );
  return 0;
}

int
ds_get_ctx_id( ds_t *h )
{
  ds_internal & ds = *(ds_internal *) h;
  return (int) ds.poll.ctx_id;
}

static void
exec_status_result( EvKeyCtx *k,  int status,  ds_msg_t *result )
{
  if ( result == NULL )
    return;
  switch ( status ) {
    case EXEC_SEND_OK: {        /* send +OK */
      static char ok[] = "OK";
      result->type   = DS_SIMPLE_STRING;
      result->len    = 2;
      result->strval = ok;
      break;
    }
    case EXEC_SEND_NIL:         /* send $-1 */
      result->type   = DS_BULK_STRING;
      result->len    = -1;
      result->strval = NULL;
      break;
    case EXEC_SEND_NULL:        /* send *-1 */
      result->type   = DS_BULK_ARRAY;
      result->len    = -1;
      result->array  = NULL;
      break;
    case EXEC_SEND_INT:         /* send :100 */
      result->type   = DS_INTEGER_VALUE;
      result->len    = 0;
      result->ival   = k->ival;
      break;
    case EXEC_SEND_ZERO:        /* send :0 */
      result->type   = DS_INTEGER_VALUE;
      result->len    = 0;
      result->ival   = 0;
      break;
    case EXEC_SEND_ZEROARR:     /* send *0 */
      result->type   = DS_BULK_ARRAY;
      result->len    = 0;
      result->array  = NULL;
      break;
    case EXEC_SEND_ONE:         /* send :1 */
      result->type   = DS_INTEGER_VALUE;
      result->len    = 0;
      result->ival   = 1;
      break;
    case EXEC_SEND_NEG_ONE:     /* send :-1 */
      result->type   = DS_INTEGER_VALUE;
      result->len    = 0;
      result->ival   = -1;
      break;
    case EXEC_SEND_ZERO_STRING: { /* send $0 */
      static char mt[] = "";
      result->type   = DS_BULK_STRING;
      result->len    = 0;
      result->strval = mt;
      break;
    }
  }
}

int
ds_run( ds_t *h,  ds_msg_t *result,  const char *str )
{
  ds_msg_t msg;
  int status = ds_parse_msg( h, &msg, str );
  if ( status == 0 )
    status = ds_run_cmd( h, result, &msg );
  return status;
}

int
ds_run_cmd( ds_t *h,  ds_msg_t *result,  ds_msg_t *cmd )
{
  ds_internal & ds = *(ds_internal *) h;
  ExecStatus    status;
#if 0
  if ( ds.exec->msg_route_cnt > ds.msg_cnt + 10000 ) {
    if ( ds_dispatch( h, 0 ) == 0 )
      ds.msg_cnt = ds.exec->msg_route_cnt;
  }
#endif
  ds.reset_pending();
  ds.exec->msg.ref( (RedisMsg &) *cmd );
  /* no key cmds are run directly */
  if ( (status = ds.exec->exec( NULL, NULL )) == EXEC_OK )
    if ( ds.alloc_fail )
      status = ERR_ALLOC_FAIL;
  switch ( status ) {
    /* cmds with keys are setup first */
    case EXEC_SETUP_OK:
      ds.exec->exec_run_to_completion();
      if ( ! ds.alloc_fail ) {
        if ( ds.exec->key_cnt == 1 ) {
          EvKeyCtx & k = ds.exec->key[ 0 ];
          if ( k.status >= EXEC_SEND_OK && k.status <= EXEC_SEND_ZERO_STRING ) {
            exec_status_result( &k, k.status, result );
            goto success;
          }
        }
        break;
      }
      status = ERR_ALLOC_FAIL;
      /* fall through */
    default:
      if ( status >= EXEC_SEND_OK && status <= EXEC_SEND_ZERO_STRING ) {
        exec_status_result( NULL, status, result );
        goto success;
      }
      /* an error that did not set up cmd or allocation failure during cmd */
      ds.exec->send_status( status, KEY_OK );
      break;
    case EXEC_QUIT:
    case EXEC_DEBUG:
      break;
  }
  if ( result != NULL ) {
    if ( ds_result( h, result ) )
      goto success;
    result->type = DS_INTEGER_VALUE;
    result->len  = 0;
    result->ival = -1;
    return -1;
  }
success:;
  /*if ( msg_cnt != ds.exec->msg_route_cnt )
    ds_dispatch( h, 0 );*/
  return 0;
}

int
ds_result( ds_t *h,  ds_msg_t *result )
{
  ds_internal & ds = *(ds_internal *) h;
  if ( ds.concat_iov() ) {
    void * buf  = ds.iov[ 0 ].iov_base;
    size_t len  = ds.iov[ 0 ].iov_len,
           len2 = len;
    /* this should be optimized, pack and unpack wastes cycles */
    if ( ((RedisMsg *) result)->unpack( buf, len, ds.tmp ) ==
         DS_MSG_STATUS_OK ) {
      ds.wr_pending       -= len;
      ds.iov[ 0 ].iov_len -= len;
      if ( len == len2 )
        ds.idx = 0;
      else
        ds.iov[ 0 ].iov_base = &((char *) buf)[ len ];
      return 1;
    }
  }
  return 0;
}

int
ds_dispatch( ds_t *h,  int ms )
{
  ds_internal & ds = *(ds_internal *) h;
  int status;

  status = ds.poll.dispatch();
  if ( status == EvPoll::DISPATCH_IDLE ) {
    ds.poll.wait( ms );
    return 0;
  }
  if ( ( status & EvPoll::POLL_NEEDED ) != 0 ) {
    ds.poll.wait( 0 );
    status = ds.poll.dispatch();
  }
  if ( ( status & EvPoll::WRITE_PRESSURE ) != 0 ) {
    do {
      status = ds.poll.dispatch();
    } while ( ( status & EvPoll::WRITE_PRESSURE ) != 0 );
    return 1;
  }
  return ( status & EvPoll::DISPATCH_BUSY ) != 0;
}

void *
ds_alloc_mem( ds_t *h,  size_t sz )
{
  ds_internal & ds = *(ds_internal *) h;
  return ds.tmp.alloc( sz );
}

int
ds_release_mem( ds_t *h )
{
  ds_internal & ds = *(ds_internal *) h;
  ds.reset();
  return 0;
}

int
ds_parse_msg( ds_t *h,  ds_msg_t *result,  const char *str )
{
  ds_internal & ds  = *(ds_internal *) h;
  RedisMsg    & msg = *(RedisMsg *) result;
  size_t        len = ::strlen( str );

  if ( msg.unpack2( (void *) str, len, ds.tmp ) == DS_MSG_STATUS_OK )
    return 0;
  return -1;
}

int
ds_msg_to_json( ds_t *h,  const ds_msg_t *msg,  ds_msg_t *json )
{
  ds_internal    & ds = *(ds_internal *) h;
  const RedisMsg & m  = *(const RedisMsg *) msg;
  size_t           sz = m.to_almost_json_size();
  char           * b  = (char *) ds.alloc( sz + 1 );

  if ( b == NULL )
    return -1;

  m.to_almost_json( b );
  b[ sz ]      = '\0';
  json->type   = DS_SIMPLE_STRING;
  json->len    = sz;
  json->strval = b;

  return 0;
}

int
ds_subscribe_with_cb( ds_t *h,  const ds_msg_t *subject,
                      ds_on_msg_t cb,  void *cl )
{
  ds_internal  & ds = *(ds_internal *) h;
  ExecStatus status = ds.exec->do_subscribe_cb( subject->strval, subject->len,
                                                cb, cl );
  if ( status == EXEC_OK )
    return 0;
  if ( status == ERR_KEY_EXISTS )
    return 1;
  return -1;
}

int
ds_psubscribe_with_cb( ds_t *h,  const ds_msg_t *subject,
                       ds_on_msg_t cb,  void *cl )
{
  ds_internal  & ds = *(ds_internal *) h;
  ExecStatus status = ds.exec->do_psubscribe_cb( subject->strval, subject->len,
                                                 cb, cl );
  if ( status == EXEC_OK )
    return 0;
  if ( status == ERR_KEY_EXISTS )
    return 1;
  return -1;
}

} /* extern "C" */

EvShmApi::EvShmApi( EvPoll &p ) noexcept
  : EvSocket( p, p.register_type( "shm_api" ) ), exec( 0 ), timer_id( 0 )
{
  this->sock_opts = kv::OPT_NO_POLL;
}

int
EvShmApi::init_exec( void ) noexcept
{
  void * e = aligned_malloc( sizeof( RedisExec ) );
  if ( e == NULL )
    return -1;
  int status, pfd = this->poll.get_null_fd();
  this->PeerData::init_ctx( pfd, this->ctx_id, "shm_api" );
  this->exec = new ( e ) RedisExec( *this->map, this->ctx_id, this->dbx_id,
                                    *this, this->poll.sub_route, *this );
  this->timer_id = ( (uint64_t) this->sock_type << 56 ) |
                   ( (uint64_t) this->ctx_id << 40 );
  this->exec->setup_ids( pfd, this->timer_id );
  if ( (status = this->poll.add_sock( this )) == 0 )
    return 0;
  return status;
}

bool
EvShmApi::timer_expire( uint64_t tid,  uint64_t event_id ) noexcept
{
  if ( this->exec != NULL && tid == this->timer_id ) {
    RedisContinueMsg *cm = NULL;
    if ( this->exec->continue_expire( event_id, cm ) ) {
      this->exec->push_continue_list( cm );
      this->idle_push( EV_PROCESS );
    }
  }
  return false;
}

void
EvShmApi::process( void ) noexcept
{
  this->pop( EV_PROCESS );
  if ( this->exec != NULL ) {
    if ( ! this->exec->cont_list.is_empty() )
      this->exec->drain_continuations( this );
  }
}

bool
EvShmApi::on_msg( EvPublish &pub ) noexcept
{
  RedisContinueMsg * cm = NULL;
  if ( this->exec != NULL ) {
    int status = this->exec->do_pub( pub, cm );
    /*if ( ( status & RPUB_FORWARD_MSG ) != 0 ) {
      already in queue
    }*/
    if ( ( status & RPUB_CONTINUE_MSG ) != 0 ) {
      this->exec->push_continue_list( cm );
      this->idle_push( EV_PROCESS );
    }
  }
  return true;
}

void
EvShmApi::process_shutdown( void ) noexcept
{
  if ( this->exec != NULL )
    this->exec->rem_all_sub();
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

bool
EvShmApi::hash_to_sub( uint32_t h,  char *key,  size_t &keylen ) noexcept
{
  return this->exec != NULL && this->exec->do_hash_to_sub( h, key, keylen );
}

void EvShmApi::write( void ) noexcept {}
void EvShmApi::read( void ) noexcept {}
void EvShmApi::release( void ) noexcept { this->StreamBuf::reset(); }
