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
RedisExec::exec_geoadd( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_geohash( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_geopos( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_geodist( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_georadius( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}

ExecStatus
RedisExec::exec_georadiusbymember( RedisKeyCtx &ctx )
{
  return EXEC_BAD_CMD;
}
