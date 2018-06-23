#ifndef __rai_raids__md_type_h__

namespace rai {
namespace ds {

enum MDType {
  MD_NODATA      = 0,
  MD_MESSAGE     = 1,
  MD_STRING      = 2,
  MD_OPAQUE      = 3,
  MD_BOOLEAN     = 4,
  MD_INT         = 5,
  MD_UINT        = 6,
  MD_REAL        = 7,
  MD_ARRAY       = 8,
  MD_PARTIAL     = 9,
  MD_IPDATA      = 10,
  MD_SUBJECT     = 11,
  MD_ENUM        = 12,
  MD_TIME        = 13,
  MD_DATE        = 14,
  MD_DATETIME    = 15,
  MD_STAMP       = 16,
  MD_DECIMAL     = 17,
  MD_LIST        = 18,
  MD_HASH        = 19,
  MD_SET         = 20,
  MD_SORTEDSET   = 21,
  MD_STREAM      = 22,
  MD_GEO         = 23,
  MD_HYPERLOGLOG = 24,
  MD_PUBSUB      = 25,
  MD_SCRIPT      = 26,
  MD_TRANSACTION = 27
};

}
}

#endif
