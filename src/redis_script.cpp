#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;

ExecStatus
RedisExec::exec_eval( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_evalsha( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_script( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

