#ifndef __rai_raids__redis_pubsub_h__
#define __rai_raids__redis_pubsub_h__

#include <raids/redis_hash.h>

namespace rai {
namespace ds {

struct SubMsgCount {
  uint32_t msg_cnt;
  SubMsgCount() : msg_cnt( 0 ) {}
};

enum SubStatus {
  SUB_OK        = 0,
  SUB_EXISTS    = 1,
  SUB_NOT_FOUND = 2
};

struct SubMap {
  HashData * h;
  SubMap() : h( 0 ) {}

  size_t sub_count( void ) const {
    if ( this->h == NULL )
      return 0;
    return this->h->hcount();
  }
  void release( void ) {
    if ( this->h != NULL )
      delete this->h;
    this->h = NULL;
  }
  /* put in new sub
   * tab[ sub ] => {cnt} */
  SubStatus put( const char *sub,  size_t len,  HashPos &pos ) {
    SubMsgCount rec;
    size_t      sz    = sizeof( rec ) + (uint8_t) len + 3,
                asize = sz;
    HashStatus  hstat = HASH_OK;
    for (;;) {
      if ( hstat == HASH_FULL || this->h == NULL )
        this->resize( asize++ );
      hstat = this->h->hsetnx( sub, len, &rec, sizeof( rec ), pos );
      if ( hstat != HASH_FULL ) {
        if ( hstat == HASH_EXISTS )
          return SUB_EXISTS;
        return SUB_OK;
      }
    }
  }
  /* update cnt for sub
   * tab[ sub ] => {cnt++} */
  SubStatus updcnt( const char *sub,  size_t len,  HashPos &pos ) const {
    ListVal     lv;
    SubMsgCount rec;
    HashStatus  hstat;

    hstat = this->h->hget( sub, len, lv, pos );
    if ( hstat == HASH_NOT_FOUND )
      return SUB_NOT_FOUND;
    lv.copy_out( &rec, 0, sizeof( rec ) );
    rec.msg_cnt++;
    lv.copy_in( &rec, 0, sizeof( rec ) );
    return SUB_OK;
  }
  /* remove tab[ sub ] */
  SubStatus rem( const char *sub,  size_t len,  HashPos &pos ) const {
    if ( this->h->hdel( sub, len, pos ) == HASH_NOT_FOUND )
      return SUB_NOT_FOUND;
    return SUB_OK;
  }
  /* add space to hash */
  void resize( size_t add_len ) {
    size_t count    = ( add_len >> 3 ) | 1,
           data_len = add_len + 1;
    if ( this->h != NULL ) {
      data_len  = add_len + this->h->data_len();
      data_len += data_len / 2 + 2;
      count     = this->h->count();
      count    += count / 2 + 2;
    }
    size_t asize = HashData::alloc_size( count, data_len );
    void * m     = ::malloc( sizeof( HashData ) + asize );
    void * p     = &((char *) m)[ sizeof( HashData ) ];
    HashData *newbe = new ( m ) HashData( p, asize );
    newbe->init( count, data_len );
    if ( this->h != NULL ) {
      this->h->copy( *newbe );
      delete this->h;
    }
    this->h = newbe;
  }
  /* iterate first tab[ sub ] */
  bool first( HashPos &pos ) const {
    if ( this->h == NULL || this->h->hcount() == 0 )
      return false;
    pos.i = 0;
    return this->next( pos );
  }
  /* iterate next tab[ sub ] */
  bool next( HashPos &pos ) const {
    HashVal kv;
    if ( this->h->hindex( ++pos.i, kv ) == HASH_OK ) {
      pos.h = kv_crc_c( kv.key, kv.keylen, 0 );
      return true;
    }
    return false;
  }
};

}
}

#endif
