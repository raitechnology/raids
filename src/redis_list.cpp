#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>
#include <raids/md_type.h>

using namespace rai;
using namespace ds;

ExecStatus
RedisExec::exec_blpop( RedisKeyCtx &ctx )
{
  /* BLPOP key [key...] timeout */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_brpop( RedisKeyCtx &ctx )
{
  /* BRPOP key [key...] timeout */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_brpoplpush( RedisKeyCtx &ctx )
{
  /* BRPOPLPUSH src dest */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lindex( RedisKeyCtx &ctx )
{
  /* LINDEX key idx */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_linsert( RedisKeyCtx &ctx )
{
  /* LINSERT key [before|after] piv val */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_llen( RedisKeyCtx &ctx )
{
  /* LLEN key */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lpop( RedisKeyCtx &ctx )
{
  /* LPOP key */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lpush( RedisKeyCtx &ctx )
{
  /* LPUSH key val [val..] */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lpushx( RedisKeyCtx &ctx )
{
  /* LPUSHX key val [val..] */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lrange( RedisKeyCtx &ctx )
{
  /* LRANGE key start stop */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lrem( RedisKeyCtx &ctx )
{
  /* LREM key count value */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_lset( RedisKeyCtx &ctx )
{
  /* LSET key idx value */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_ltrim( RedisKeyCtx &ctx )
{
  /* LTRIM key start stop */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_rpop( RedisKeyCtx &ctx )
{
  /* RPOP key */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_rpoplpush( RedisKeyCtx &ctx )
{
  /* RPOPLPUSH src dest */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_rpush( RedisKeyCtx &ctx )
{
  /* RPUSH key [val..] */
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_rpushx( RedisKeyCtx &ctx )
{
  /* RPUSHX key [val..] */
  return EXEC_BAD_CMD;
}

