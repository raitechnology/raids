#ifndef __rai_raids__redis_keyspace_h__
#define __rai_raids__redis_keyspace_h__

#include <raikv/route_db.h>

namespace rai {
namespace ds {

struct RedisExec;

struct RedisKeyspace {
  RedisExec  & exec;
  const char * key;
  size_t       keylen;
  const char * evt;
  size_t       evtlen;
  char       * subj,
             * ptr;
  size_t       alloc_len;
  char         db[ 4 ];

  RedisKeyspace( RedisExec &e ) : exec( e ), keylen( 0 ), evtlen( 0 ),
                                  subj( 0 ), ptr( 0 ), alloc_len( 0 ) {
    this->db[ 0 ] = 0;
  }
  /* alloc temp subject space for __key...@db__:xxx subject */
  bool alloc_subj( size_t subj_len ) noexcept;
  /* fill in db[] */
  size_t db_str( size_t off ) noexcept;
  /* append "@db__:" to subj */
  size_t db_to_subj( size_t off ) noexcept;
  /* a keyspace like subject, make_*_subj() below, * must be 8 chars */
  size_t make_bsubj( const char *blk ) noexcept;
  /* create a subject: __keyspace@db__:key */
  size_t make_keyspace_subj( void ) { return this->make_bsubj( "__keyspace" ); }
  /* create a subject: __listblkd@db__:key */
  size_t make_listblkd_subj( void ) { return this->make_bsubj( "__listblkd" ); }
  /* create a subject: __zsetblkd@db__:key */
  size_t make_zsetblkd_subj( void ) { return this->make_bsubj( "__zsetblkd" ); }
  /* create a subject: __strmblkd@db__:key */
  size_t make_strmblkd_subj( void ) { return this->make_bsubj( "__strmblkd" ); }
  /* forward a keyspace like msg below, fwd_*() */
  bool fwd_bsubj( const char *blk ) noexcept;
  /* publish __keyspace@N__:key <- event */
  bool fwd_keyspace( void ) { return this->fwd_bsubj( "__keyspace" ); }
  /* publish __listblkd@N__:key <- event */
  bool fwd_listblkd( void ) { return this->fwd_bsubj( "__listblkd" ); }
  /* publish __zsetblkd@N__:key <- event */
  bool fwd_zsetblkd( void ) { return this->fwd_bsubj( "__zsetblkd" ); }
  /* publish __strmblkd@N__:key <- event */
  bool fwd_strmblkd( void ) { return this->fwd_bsubj( "__strmblkd" ); }
  /* publish __keyevent@N__:event <- key  (different than :key <- event above)*/
  bool fwd_keyevent( void ) noexcept;
  /* publish __monitor_@N__ <- cmd */
  bool fwd_monitor( void ) noexcept;
  /* convert command into keyspace events and publish them */
  static bool pub_keyspace_events( RedisExec &e ) noexcept;
  static void init_keyspace_events( RedisExec &e ) noexcept;
};

struct RedisKeyspaceNotify : public kv::RouteNotify {
  /* this should be moved to raids */
  uint32_t keyspace, /* route of __keyspace@N__ subscribes active */
           keyevent, /* route of __keyevent@N__ subscribes active */
           listblkd, /* route of __listblkd@N__ subscribes active */
           zsetblkd, /* route of __zsetblkd@N__ subscribes active */
           strmblkd, /* route of __strmblkd@N__ subscribes active */
           monitor;  /* route of __monitor_@N__ subscribes active */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  RedisKeyspaceNotify( kv::RoutePublish &p ) : kv::RouteNotify( p ),
    keyspace( 0 ), keyevent( 0 ), listblkd( 0 ), zsetblkd( 0 ), strmblkd( 0 ),
    monitor( 0 ) {
    /*this->notify_type = 'R';*/
  }
  void update_keyspace_route( uint32_t &val,  uint16_t bit,
                              int add,  uint32_t fd ) noexcept;
  void update_keyspace_count( const char *sub,  size_t len,
                              int add,  uint32_t fd ) noexcept;
  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_reassert( uint32_t fd,  kv::RouteVec<kv::RouteSub> &sub_db,
                            kv::RouteVec<kv::RouteSub> &pat_db ) noexcept;
};

}
}

#endif
