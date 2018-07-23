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
RedisExec::exec_sadd( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_scard( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sdiff( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sdiffstore( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sinter( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sinterstore( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sismember( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_smembers( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_smove( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_spop( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_srandmember( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_srem( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sunion( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sunionstore( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_sscan( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

