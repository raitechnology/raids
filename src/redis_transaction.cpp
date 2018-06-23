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
RedisExec::exec_discard( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_exec( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_multi( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_unwatch( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_watch( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

