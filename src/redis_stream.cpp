#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raikv/util.h>
#include <raids/redis_exec.h>

using namespace rai;
using namespace ds;

ExecStatus
RedisExec::exec_xadd( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xlen( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xrange( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xrevrange( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xread( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xreadgroup( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xgroup( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xack( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xpending( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xclaim( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xinfo( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

ExecStatus
RedisExec::exec_xdel( EvKeyCtx &/*ctx*/ )
{
  return ERR_BAD_CMD;
}

