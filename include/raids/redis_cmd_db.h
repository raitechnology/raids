#ifndef __rai_raids__redis_cmd_db_h__
#define __rai_raids__redis_cmd_db_h__

namespace rai {
namespace ds {

struct CommandDB {
  const char * name,
             * descr,
             * attr;
};

extern CommandDB cmd_db[];
extern const size_t cmd_db_cnt;

extern const char *cmd_flag[];
extern const size_t cmd_flag_cnt;

}
}

#endif
