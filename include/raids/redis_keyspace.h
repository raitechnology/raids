#ifndef __rai_raids__redis_keyspace_h__
#define __rai_raids__redis_keyspace_h__

namespace rai {
namespace ds {

struct RedisExec;

struct RedisKeyspace {
  RedisExec  & exec;
  const char * key;
  size_t       keylen;
  const char * evt;
  size_t       evtlen;
  char       * subj;
  size_t       alloc_len;
  char         db[ 4 ];

  RedisKeyspace( RedisExec &e ) : exec( e ), keylen( 0 ), evtlen( 0 ),
                                  subj( 0 ), alloc_len( 0 ) {
    this->db[ 0 ] = 0;
  }
  /* alloc temp subject space for __key...@db__:xxx subject */
  bool alloc_subj( size_t subj_len );
  /* fill in db[] */
  size_t db_str( size_t off );
  /* append "@db__:" to subj */
  size_t db_to_subj( size_t off );
  /* create a subject: <prefix>@db__:key */
  /*bool make_subj( const char *prefix,  size_t pre_len );*/
  size_t make_keyspace_subj( void );
  /* publish __keyspace@N__:key <- event */
  bool fwd_keyspace( void );
  /* publish __keyevent@N__:event <- key */
  bool fwd_keyevent( void );
  /* convert command into keyspace events and publish them */
  static bool pub_keyspace_events( RedisExec &e );
};

}
}

#endif
