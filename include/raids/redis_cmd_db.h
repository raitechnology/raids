#ifndef __rai_raids__redis_cmd_db_h__
#define __rai_raids__redis_cmd_db_h__

namespace rai {
namespace ds {

enum RedisExtraType {
  XTRA_SHORT   = 0, /* short description, around 20 chars */
  XTRA_USAGE   = 1, /* usage of cmd: scan curs [match pat] [count int] */
  XTRA_EXAMPLE = 2, /* example of usage */
  XTRA_DESCR   = 3, /* long description */
  XTRA_RETURN  = 4  /* description of return value */
};

struct RedisCmdExtra {
  const RedisCmdExtra * next; /* list of extra sections */
  RedisExtraType type; /* type above */
  const char   * text; /* text of short, usage, example, descr, return */
};

enum RedisCmdFlags {
  CMD_NOFLAGS       = 0,
  CMD_ADMIN_FLAG    = 1, /* bits attached to command in RedisCmdData::flags */
  CMD_READ_FLAG     = 2,
  CMD_WRITE_FLAG    = 4,
  CMD_MOVABLE_FLAG  = 8,
  CMD_READ_MV_FLAG  = CMD_READ_FLAG | CMD_MOVABLE_FLAG,
  CMD_WRITE_MV_FLAG = CMD_WRITE_FLAG | CMD_MOVABLE_FLAG
};

static const size_t MAX_CMD_LEN  = 32, /* strlen( "MGET" ) + 1 */
                    MAX_CATG_LEN = 24; /* strlen( "STRING" ) + 1 */

struct RedisCmdData {
  const char * name; /* string name of command (lower case) */
  const RedisCmdExtra * extra; /* extra description for humans */
  uint32_t hash,   /* crc_c( mget, seed ) */
           cmd;    /* MGET_CMD */
  uint16_t flags;  /* CMD_READ_FLAG */
  uint8_t  cmdlen, /* strlen( mget ) */
           catg;   /* STRING_CATG */
  int8_t   arity,  /*  -2 | 1 | -1 | 1 */
           first,
           last,
           step;

  const RedisCmdExtra * get_extra( RedisExtraType t ) const {
    const RedisCmdExtra * ex = this->extra;
    while ( ex != NULL && ex->type != t )
      ex = ex->next;
    return ex;
  }
};

}
}

#endif
