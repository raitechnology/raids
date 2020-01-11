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
  /* a keyspace like subject, make_*_subj() below, * must be 8 chars */
  size_t make_bsubj( const char *blk );
  /* create a subject: __keyspace@db__:key */
  size_t make_keyspace_subj( void ) { return this->make_bsubj( "__keyspace" ); }
  /* create a subject: __listblkd@db__:key */
  size_t make_listblkd_subj( void ) { return this->make_bsubj( "__listblkd" ); }
  /* create a subject: __zsetblkd@db__:key */
  size_t make_zsetblkd_subj( void ) { return this->make_bsubj( "__zsetblkd" ); }
  /* create a subject: __strmblkd@db__:key */
  size_t make_strmblkd_subj( void ) { return this->make_bsubj( "__strmblkd" ); }
  /* forward a keyspace like msg below, fwd_*() */
  bool fwd_bsubj( const char *blk );
  /* publish __keyspace@N__:key <- event */
  bool fwd_keyspace( void ) { return this->fwd_bsubj( "__keyspace" ); }
  /* publish __listblkd@N__:key <- event */
  bool fwd_listblkd( void ) { return this->fwd_bsubj( "__listblkd" ); }
  /* publish __zsetblkd@N__:key <- event */
  bool fwd_zsetblkd( void ) { return this->fwd_bsubj( "__zsetblkd" ); }
  /* publish __strmblkd@N__:key <- event */
  bool fwd_strmblkd( void ) { return this->fwd_bsubj( "__strmblkd" ); }
  /* publish __keyevent@N__:event <- key  (different than :key <- event above)*/
  bool fwd_keyevent( void );
  /* publish __monitor_@N__ <- cmd */
  bool fwd_monitor( void );
  /* convert command into keyspace events and publish them */
  static bool pub_keyspace_events( RedisExec &e );
};

}
}

#endif
