#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/redis_cmd_db.h>

using namespace rai;
using namespace ds;
using namespace kv;

ExecStatus
RedisExec::exec_get( void )
{
  const char * key;
  char       * str;
  size_t       keylen;
  uint64_t     h1, h2,
               data_sz;
  void       * data;
  size_t       sz;

  if ( ! this->msg.get_arg( 1, key, keylen ) )
    return EXEC_BAD_ARGS;
  this->kb.keylen = keylen + 1;
  ::memcpy( this->kb.u.buf, key, keylen );
  this->kb.u.buf[ keylen ] = '\0';
  h1 = this->seed;
  h2 = this->seed2;
  this->kb.hash( h1, h2 );
  this->kctx.set_hash( h1, h2 );
  if ( (this->kstatus = this->kctx.find( &this->wrk )) == KEY_OK ) {
    this->kstatus = this->kctx.value( &data, data_sz );
    if ( this->kstatus == KEY_OK ) {
      sz = 16 + data_sz;
      str = this->out.alloc( sz );
      if ( str != NULL ) {
        str[ 0 ] = '$';
        sz = 1 + uint64_to_string( data_sz, &str[ 1 ] );
        str[ sz ] = '\r'; str[ sz + 1 ] = '\n';
        ::memcpy( &str[ sz + 2 ], data, data_sz );
        sz += 2 + data_sz;
        str[ sz ] = '\r'; str[ sz + 1 ] = '\n';
        this->out.sz += sz + 2;
      }
      return EXEC_OK;
    }
  }
  if ( this->kstatus == KEY_NOT_FOUND ) {
    static char not_found[] = "$-1\r\n";
    this->out.append( not_found, 5 );
    return EXEC_OK;
  }
  return EXEC_KV_STATUS;
}

ExecStatus
RedisExec::exec_set( void )
{
  const char * key, * value;
  size_t       keylen, valuelen;
  uint64_t     h1, h2;
  void       * data;

  if ( ! this->msg.get_arg( 1, key, keylen ) ||
       ! this->msg.get_arg( 2, value, valuelen ) )
    return EXEC_BAD_ARGS;

  this->kb.keylen = keylen + 1;
  ::memcpy( this->kb.u.buf, key, keylen );
  this->kb.u.buf[ keylen ] = '\0';
  h1 = this->seed;
  h2 = this->seed2;
  this->kb.hash( h1, h2 );
  this->kctx.set_hash( h1, h2 );
  if ( (this->kstatus = this->kctx.acquire( &this->wrk )) <= KEY_IS_NEW ) {
    this->kstatus = this->kctx.resize( &data, valuelen );
    if ( this->kstatus == KEY_OK )
      ::memcpy( data, value, valuelen );
    this->kctx.release();
    if ( this->kstatus == KEY_OK )
      return EXEC_SEND_OK;
  }
  return EXEC_KV_STATUS;
}

ExecStatus
RedisExec::exec_command( void )
{
  RedisMsg     m;
  const char * name;
  size_t       j = 0, len;
  RedisCmd     cmd;

  this->mstatus = REDIS_MSG_OK;
  switch ( this->msg.match_arg( 1, "info",    4,
                                   "getkeys", 7,
                                   "count",   5,
                                   "help",    4, NULL ) ) {
    case 0: { /* no args */
      if ( ! m.alloc_array( this->out.out, REDIS_CMD_COUNT - 1 ) )
        return EXEC_ALLOC_FAIL;
      for ( size_t i = 1; i < REDIS_CMD_COUNT; i++ ) {
        this->mstatus = m.array[ j++ ].unpack_json( cmd_db[ i ].attr,
                                                    this->out.out );
        if ( this->mstatus != REDIS_MSG_OK )
          break;
      }
      m.len = j;
      break;
    }
    case 1: { /* info */
      if ( ! m.alloc_array( this->out.out, this->msg.len - 2 ) )
        return EXEC_ALLOC_FAIL;
      if ( m.len > 0 ) {
        for ( int i = 2; this->msg.get_arg( i, name, len ); i++ ) {
          cmd = get_upper_cmd( name, len );
          m.array[ j++ ].unpack_json( cmd_db[ cmd ].attr, this->out.out );
        }
        m.len = j;
      }
      break;
    }
    case 2: /* getkeys */
      return EXEC_BAD_ARGS;
    case 3: /* count */
      m.set_int( REDIS_CMD_COUNT - 1 );
      break;
    case 4: { /* help */
      if ( ! m.alloc_array( this->out.out, this->msg.len * 2 ) )
        return EXEC_ALLOC_FAIL;
      for ( int i = 2; this->msg.get_arg( i, name, len ); i++ ) {
        cmd = get_upper_cmd( name, len );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ cmd ].name );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ cmd ].descr );
      }
      if ( j == 0 ) {
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ COMMAND_CMD ].name );
        m.array[ j++ ].set_simple_string( (char *) cmd_db[ COMMAND_CMD ].descr);
      }
      m.len = j;
      break;
    }
    default:
      return EXEC_BAD_ARGS;
  }
  if ( this->mstatus == REDIS_MSG_OK ) {
    size_t sz  = 16 * 1024;
    void * buf = this->out.alloc( sz );
    if ( buf == NULL )
      return EXEC_ALLOC_FAIL;
    this->mstatus = m.pack( buf, sz );
    if ( this->mstatus == REDIS_MSG_OK )
      this->out.append_iov( buf, sz );
  }
  if ( this->mstatus != REDIS_MSG_OK )
    return EXEC_MSG_STATUS;
  return EXEC_OK;
}

ExecStatus
RedisExec::exec_ping( void )
{
  if ( this->argc > 1 ) {
    RedisMsg &sub = this->msg.array[ 1 ];
    size_t sz  = sub.len + 32;
    void * buf = this->out.alloc( sz );
    if ( buf != NULL ) {
      sub.pack( buf, sz );
      this->out.sz += sz;
    }
  }
  else {
    static char pong[] = "+PONG\r\n";
    this->out.append( pong, 7 );
  }
  return EXEC_OK;
}

