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
RedisExec::exec_eval( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_evalsha( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_script( RedisKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

