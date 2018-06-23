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
RedisExec::exec_psubscribe( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_pubsub( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_publish( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_punsubscribe( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_subscribe( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_unsubscribe( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

