#ifndef __rai_raids__nats_map_h__
#define __rai_raids__nats_map_h__

#include <raids/redis_hash.h>

namespace rai {
namespace ds {

/*
 * Sid can have only one subject in NATS, but can have multiple subs here
 * a subject may have multipe sids
 *
 *   sub_tab [ subject ] => SidMsgCount+SID, SidMsgCount+SID, ...
 *
 *   sid_tab [ SID ] => SidMsgCount+subject, SidMsgCount+subject, ...
 *
 * The sid_tab contains both max_msgs and msg_cnt, but only max_msgs is used,
 * to initialize the sub_tab subject max_msgs.  The max_msgs refers to msgs on
 * a single subject, not the sum of all subjects msgs attached to a sid.
 *
 * The sub_tab sid tracks both msg counts and triggers the unsubscribe when
 * msg_cnt expires at msg_cnt >= max_msgs in incr_msg_cnt().
 */
struct SidMsgCount {
  uint32_t max_msgs, /* if unsubscribe sid max_msgs */
           msg_cnt;  /* current msg count through sid */

  SidMsgCount( uint32_t maxm ) : max_msgs( maxm ), msg_cnt( 0 ) {}
};

struct StrHashRec {
  uint8_t len; /* 0 -> 255 */
  char    str[ 255 ];
  StrHashRec() : len( 0 ) {}

  bool empty( void ) const { return this->len == 0; }
  size_t length( void ) const { return (size_t) this->len + 1; }
  bool equals( const StrHashRec &sr ) const {
    return this->len == sr.len && ::memcmp( this->str, sr.str, this->len ) == 0;
  }
  uint32_t hash( void ) const {
    return kv_crc_c( this->str, this->len, 0 );
  }
  static StrHashRec &make_rec( char *s,  size_t len ) {
    *--s = (uint8_t) len;
    return *(StrHashRec *) (void *) s;
  }
};

struct StrHashKey {
  StrHashRec & rec;
  HashPos      pos;
  StrHashKey( StrHashRec &r ) : rec( r ), pos( r.hash() ) {}
};

struct SidIter : public ListVal {
  size_t off, len, end;
  SidIter() : off( 0 ) {}

  void copy( const HashVal &kv ) {
    *(ListVal *) this = kv;
  }
  void init_first( void ) {
    this->end = 0;
    this->len = 0;
  }

  bool first( void ) {
    this->init_first();
    return this->next();
  }

  bool next( void ) {
    this->off = end;
    if ( this->off >= this->length() )
      return false;
    this->len = this->get_byte( this->off + sizeof( SidMsgCount ) );
    this->end = this->off + sizeof( SidMsgCount ) + 1 + this->len;
    return true;
  }
  
  size_t key_off( void ) {
    return this->off + sizeof( SidMsgCount ) + 1;
  }
};

struct SidList {
  StrHashRec  sid;
  SidIter     lv;
  SidMsgCount cnt;
  SidList() : cnt( 0 ) {}

  bool incr_msg_count( void ) {
    size_t cnt_off = this->lv.off + sizeof( uint32_t );
    this->cnt.msg_cnt++;
    this->lv.copy_in( &this->cnt.msg_cnt, cnt_off, sizeof( uint32_t ) );
    return this->cnt.max_msgs == 0 || this->cnt.msg_cnt < this->cnt.max_msgs;
  }
  bool first( void ) {
    this->lv.init_first();
    return this->next();
  }
  bool next( void ) {
    if ( this->lv.next() ) {
      this->lv.copy_out( &this->cnt, this->lv.off, sizeof( SidMsgCount ) );
      this->sid.len = this->lv.len;
      if ( this->sid.len != 0 ) {
        this->lv.copy_out( this->sid.str, this->lv.key_off(), this->sid.len );
        return true;
      }
    }
    return false;
  }
};

enum NatsSubStatus {
  NATS_OK         = 0,
  NATS_IS_NEW     = 1,
  NATS_IS_EXPIRED = 2
};

struct NatsMap {
  HashData * h;
  NatsMap() : h( 0 ) {}
  void release( void ) {
    if ( this->h != NULL )
      delete this->h;
    this->h = NULL;
  }
  /* tab[ sub ] => [{cnt, sid}, ...] */
  void print( void ) const {
    size_t cnt = ( this->h == NULL ? 0 : this->h->hcount() );
    SidList sid_list;
    HashVal kv;
    for ( size_t i = 0; i < cnt; i++ ) {
      this->h->hindex( i+1, kv );
      printf( "%.*s: ", (int) kv.keylen, kv.key );
      sid_list.lv.copy( kv );
      printf( "(sz:%lu) ", kv.sz + kv.sz2 );
      if ( sid_list.first() ) {
        do {
          printf( "{%u/%u} %.*s ",
                  sid_list.cnt.msg_cnt, sid_list.cnt.max_msgs,
                  (int) sid_list.sid.len, sid_list.sid.str );
        } while ( sid_list.next() );
      }
      printf( "\n" );
    }
  }
  /* put in any elem, search for it and append if not found
   * tab[ sub ] => {cnt, sid}, {cnt, sid} */
  NatsSubStatus put( StrHashKey &key,  SidMsgCount &cnt,  StrHashRec &rec,
                     bool copy_max_msgs = false ) {
    SidIter       lv;
    size_t        sz    = sizeof( SidMsgCount ) + rec.length(),
                  asize = sz + key.rec.length() + 3;
    HashStatus    hstat = HASH_OK;
    NatsSubStatus nstat = NATS_OK;
    do {
      if ( hstat == HASH_FULL || this->h == NULL )
        this->resize( asize++ );
      hstat = this->h->hget( key.rec.str, key.rec.len, lv, key.pos );
      if ( hstat == HASH_NOT_FOUND ) {
        nstat = NATS_IS_NEW;
      }
      else {
        /* search for sub/sid */
        if ( lv.first() ) {
          do {
            /* if matches existing or is null entry */
            if ( lv.len == 0 ||
                 ( lv.len == rec.len &&
                   lv.cmp( rec.str, lv.key_off(), lv.len ) == 0 ) ) {
              SidMsgCount tmp( 0 );
              lv.copy_out( &tmp, lv.off, sizeof( SidMsgCount ) );
              cnt.msg_cnt = tmp.msg_cnt;
              /* check of max_msgs causes sub to expire */
              if ( cnt.max_msgs > 0 ) {
                lv.copy_in( &cnt, lv.off, sizeof( uint32_t ) );
                if ( cnt.max_msgs <= cnt.msg_cnt )
                  nstat = NATS_IS_EXPIRED;
              }
              else {
                cnt.max_msgs = tmp.max_msgs;
              }
              if ( lv.len != 0 )
                return nstat; /* exists or is expired */
              break;
            }
            else if ( copy_max_msgs ) {
              if ( cnt.max_msgs == 0 ) {
                uint32_t max_msgs;
                lv.copy_out( &max_msgs, lv.off, sizeof( uint32_t ) );
                if ( max_msgs != 0 )
                  cnt.max_msgs = max_msgs;
              }
              copy_max_msgs = false;
            }
          } while ( lv.next() );
        }
      }
      /* allocate new space for sub */
      hstat = this->h->halloc( key.rec.str, key.rec.len, sz + lv.off, lv,
                               key.pos );
      if ( hstat == HASH_OK ) {
        lv.copy_in( &cnt, lv.off, sizeof( SidMsgCount ) );
        lv.copy_in( &rec, lv.off + sizeof( SidMsgCount ), rec.length() );
      }
    } while ( hstat == HASH_FULL );
    return nstat;
  }
  /* update cnt for each sid or add it if it doesn't exist
   * tab[ sub ] => {cnt, sid}, {cnt, sid} */
  NatsSubStatus updcnt( StrHashKey &key,  SidMsgCount &cnt,
                        StrHashRec *match ) {
    SidIter       lv;
    size_t        sz    = sizeof( SidMsgCount ) + 1,
                  asize = sz + 3;
    HashStatus    hstat = HASH_OK;
    NatsSubStatus nstat = NATS_OK;
    do {
      if ( hstat == HASH_FULL || this->h == NULL )
        this->resize( asize++ );
      hstat = this->h->hget( key.rec.str, key.rec.len, lv, key.pos );
      if ( hstat == HASH_NOT_FOUND ) {
        nstat = NATS_IS_NEW;
        /* allocate new space for zero length placeholder */
        hstat = this->h->halloc( key.rec.str, key.rec.len, sz, lv, key.pos );
        if ( hstat == HASH_OK ) {
          lv.copy_in( &cnt, 0, sizeof( SidMsgCount ) );
          lv.put_byte( sizeof( SidMsgCount ), 0 );
          return NATS_IS_NEW;
        }
      }
      else {
        NatsSubStatus nstat = NATS_OK;
        if ( lv.first() ) {
          do {
            if ( match == NULL ||
                 ( match->len == lv.len &&
                   lv.cmp( match->str, lv.key_off(), lv.len ) == 0 ) ) {
              /* if matches existing */
              SidMsgCount tmp( 0 );
              lv.copy_out( &tmp, lv.off, sizeof( SidMsgCount ) );
              cnt.msg_cnt = tmp.msg_cnt;
              /* check of max_msgs causes sub to expire */
              if ( cnt.max_msgs > 0 ) {
                lv.copy_in( &cnt, lv.off, sizeof( uint32_t ) );
                if ( cnt.max_msgs <= cnt.msg_cnt )
                  nstat = NATS_IS_EXPIRED;
              }
              else {
                cnt.max_msgs = tmp.max_msgs;
              }
              if ( match != NULL )
                break;
            }
          } while ( lv.next() );
        }
        return nstat; /* exists or is expired */
      }
    } while ( hstat == HASH_FULL );
    return nstat;
  }
  /* tab[ sub ] => [{cnt, sid}, ...] */
  bool lookup( const char *sub,  size_t len,  HashPos &pos,
               SidList &sid ) const {
    if ( this->h == NULL )
      return false;
    return this->h->hget( sub, len, sid.lv, pos ) == HASH_OK;
  }
  /* tab[ sub ] => [{cnt, sid}, ...] */
  bool lookup( StrHashKey &key,  SidList &sid ) const {
    if ( this->h == NULL )
      return false;
    return this->h->hget( key.rec.str, key.rec.len, sid.lv, key.pos )== HASH_OK;
  }
  /* get the first entry, skipping over counters */
  bool get_expired( const char *sub,  size_t len,  HashPos &pos,
                    StrHashRec &rec ) const {
    SidIter     lv;
    SidMsgCount cnt( 0 );
    rec.len = 0;
    if ( this->h == NULL )
      return false;
    if ( this->h->hget( sub, len, lv, pos ) == HASH_NOT_FOUND )
      return false;
    if ( lv.first() ) {
      do {
        lv.copy_out( &cnt, lv.off, sizeof( SidMsgCount ) );
        if ( cnt.max_msgs > 0 && cnt.msg_cnt >= cnt.max_msgs ) {
          rec.len = lv.len;
          lv.copy_out( rec.str, lv.off + sizeof( SidMsgCount ) + 1, lv.len );
          return true;
        }
      } while ( lv.next() );
    }
    return false;
  }
  /* tab[ sub ] => tab[ sub ] - {sid} */
  bool deref( StrHashKey &key,  StrHashRec &rec ) const {
    SidIter    lv;
    HashStatus hstat;

    if ( this->h == NULL )
      return false;
    hstat = this->h->hget( key.rec.str, key.rec.len, lv, key.pos );
    if ( hstat == HASH_NOT_FOUND )
      return false;
    if ( lv.first() ) {
      do {
        if ( lv.len == rec.len &&
             lv.cmp( rec.str, lv.off + sizeof( SidMsgCount ) + 1,
                     lv.len ) == 0 ) {
          for ( size_t j = lv.end; j < lv.length(); j += 256 ) {
            char   tmp[ 256 ];
            size_t sz = lv.copy_out( tmp, j, sizeof( tmp ) );
            lv.copy_in( tmp, lv.off, sz );
            lv.off += sz;
          }
          if ( lv.off == 0 ) {
            this->h->hrem( key.pos.i );
            return true;
          }
          this->h->halloc( key.rec.str, key.rec.len, lv.off, lv, key.pos );
          return false;
        }
      } while ( lv.next() );
    }
    return false;
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
  /* remove tab[ sid ] after using get() */
  void rem( StrHashKey &key ) const {
    this->h->hrem( key.pos.i );
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
};

}
}

#endif

