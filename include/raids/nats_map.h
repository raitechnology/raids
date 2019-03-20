#ifndef __rai_raids__nats_map_h__
#define __rai_raids__nats_map_h__

extern "C" {
  struct pcre2_real_code_8;
  struct pcre2_real_match_data_8;
}

#include <raids/route_ht.h>

namespace rai {
namespace ds {

/*
 * Sid can have only one subject in NATS, but can have multiple subs here
 * a subject may have multipe sids
 *
 *   sub_tab [ subject, max_msgs, msg_cnt ] => sid1, [ sid2, ... ]
 *
 *   sid_tab [ sid1, max_msgs ] => subject1, [ subject2, ... ]
 *
 * The sid_tab contains both max_msgs and msg_cnt, but only max_msgs is used,
 * to initialize the sub_tab subject max_msgs.  The max_msgs refers to msgs on
 * a single subject, not the sum of all subjects msgs attached to a sid.
 *
 * The sub_tab sid tracks both msg counts and triggers the unsubscribe when
 * msg_cnt expires at msg_cnt >= max_msgs.
 */
struct SidMsgCount {
  uint32_t max_msgs, /* if unsubscribe sid max_msgs */
           msg_cnt;  /* current msg count through sid */

  SidMsgCount( uint32_t maxm ) : max_msgs( maxm ), msg_cnt( 0 ) {}
};

/* a subject or sid string */
struct NatsStr {
  const char * str; /* the data */
  uint16_t     len; /* array len of str[] */
  uint32_t     h;   /* cached hash value */

  NatsStr( const char *s = NULL,  uint16_t l = 0,  uint32_t ha = 0 )
    : str( s ), len( l ), h( ha ) {}

  uint32_t hash( void ) {
    if ( this->h == 0 )
      this->h = kv_crc_c( this->str, this->len, 0 );
    return this->h;
  }

  size_t copy( char *out ) const {
    ::memcpy( out, &this->len, sizeof( uint16_t ) );
    ::memcpy( &out[ sizeof( uint16_t ) ], this->str, this->len );
    return this->len + sizeof( uint16_t );
  }

  size_t read( const char *val ) {
    ::memcpy( &this->len, val, sizeof( uint16_t ) );
    this->str = &val[ sizeof( uint16_t ) ];
    this->h   = 0;
    return this->len + sizeof( uint16_t );
  }

  bool equals( const char *val ) const {
    uint16_t vallen;
    ::memcpy( &vallen, val, sizeof( uint16_t ) );
    return vallen == this->len &&
           ::memcmp( &val[ sizeof( uint16_t ) ], this->str, this->len ) == 0;
  }

  bool equals( const NatsStr &val ) const {
    return val.len == this->len && ::memcmp( val.str, this->str, val.len ) == 0;
  }

  size_t length( void ) const {
    return sizeof( uint16_t ) + this->len;
  }

  bool is_wild( void ) const {
    if ( this->len == 0 )
      return false;
    /* last char or fist char * or > */
    if ( this->str[ this->len - 1 ] == '*' ||
         this->str[ this->len - 1 ] == '>' ) {
      if ( this->len == 1 ) /* is first char */
        return true;
      if ( this->str[ this->len - 2 ] == '.' ) /* is last char */
        return true;
    }
    /* look for .*. */
    return ::memmem( this->str, this->len, ".*.", 3 ) != NULL;
  }
  bool is_valid( void ) {
    if ( this->len == 0 )
      return false;
    /* if first is . or last is . */
    if ( this->str[ 0 ] == '.' || this->str[ this->len - 1 ] == '.' )
      return false;
    /* if any empty segments */
    return ::memmem( this->str, this->len, "..", 2 ) == NULL;
  }
};

/* a struct used to insert or find a sid/subj */
struct NatsPair {
  NatsStr & one, /* the key */
          * two; /* the value inserted, null on find */

  NatsPair( NatsStr &o,  NatsStr *t = NULL ) : one( o ), two( t ) {}

  size_t length( void ) const {
    return this->one.length() + ( this->two ? this->two->length() : 0 );
  }
  bool equals( const char *s ) const {
    return this->one.equals( s );
  }
  void copy( char *s ) const { /* init a list, key and data */
    size_t off = this->one.copy( s );
    if ( this->two ) this->two->copy( &s[ off ] );
  }
};

/* a record of a subject or sid */
struct NatsMapRec {
  uint32_t hash,       /* hash value of the sub/sid */
           msg_cnt,    /* the current count of msgs published */
           max_msgs;   /* the maximum msg count, set with unsub sid <max> */
  uint16_t len;        /* size of list */
  char     value[ 2 ]; /* first element is the key, the rest are a list */

  bool equals( const void *s,  uint16_t ) const {
    return ((const NatsPair *) s)->equals( this->value );
  }
  void copy( const void *s,  uint16_t ) {
    return ((const NatsPair *) s)->copy( this->value );
  }
};

/* hash of prefix with wild value */
struct NatsWildRec {
  uint32_t                  hash,
                            subj_hash;
  pcre2_real_code_8       * re;
  pcre2_real_match_data_8 * md;
  uint16_t                  len;
  char                      value[ 2 ];

  bool equals( const void *s,  uint16_t l ) const {
    return l == this->len && ::memcmp( s, this->value, l ) == 0;
  }
  void copy( const void *s,  uint16_t l ) {
    ::memcpy( this->value, s, l );
  }
};

enum NatsSubStatus {
  NATS_OK        = 0,
  NATS_IS_NEW    = 1,
  NATS_EXPIRED   = 2,
  NATS_NOT_FOUND = 3,
  NATS_EXISTS    = 4,
  NATS_TOO_MANY  = 5
};

/* the subscription table map hashing methods */
typedef RouteVec<NatsMapRec>  NatsMap;
typedef RouteVec<NatsWildRec> NatsWild;

/* iterate over sid -> subjects or subject -> sids */
struct NatsIter {
  char   * buf;
  uint16_t buflen,
           off,
           curlen,
           rmlen;

  NatsIter() : buf( 0 ), buflen( 0 ), off( 0 ), curlen( 0 ), rmlen( 0 ) {}

  void init( NatsMapRec &rt ) {
    this->init( rt.value, ((uint16_t *) (void *) rt.value)[ 0 ] + 2, rt.len );
  }
  void init( char *s,  uint16_t o,  uint16_t l ) {
    this->buf    = s;
    this->buflen = l;
    this->off    = o;
  }
  bool get( NatsStr &str ) {
    if ( this->off >= this->buflen )
      return false;
    this->curlen = str.read( &this->buf[ this->off ] );
    return true;
  }
  void incr( void ) {
    this->off += this->curlen;
  }
  void remove( void ) {
    this->buflen -= this->curlen;
    this->rmlen  += this->curlen;
    ::memmove( &this->buf[ this->off ],
               &this->buf[ this->off + this->curlen ],
               this->buflen - this->off );
  }
  bool is_member( const NatsStr &val ) {
    NatsStr mem;
    for ( ; this->get( mem ); this->incr() ) {
      if ( mem.equals( val ) )
        return true;
    }
    return false;
  }
};

/* used for expire contexts after publish or unsub */
struct NatsLookup {
  NatsIter     iter; /* list of subs / sids that are published or unsubed */
  RouteLoc     loc;  /* location of the sid / subject */
  NatsMapRec * rt;   /* the subject / sid record, containing the list */

  NatsLookup() : rt( 0 ) {
    this->loc.init();
  }
};

/* table of subjects and sids for routing messages */
struct NatsSubMap {
  NatsMap  sub_map,
           sid_map;
  NatsWild wild_map;

  void release( void ) {
    this->sub_map.release();
    this->sid_map.release();
    this->wild_map.release();
  }
  NatsWildRec *add_wild( uint32_t h,  NatsStr &subj ) {
    RouteLoc      loc;
    NatsWildRec * rt;
    rt = this->wild_map.upsert( h, subj.str, subj.len, loc );
    if ( rt != NULL && loc.is_new ) {
       rt->subj_hash = subj.hash();
       rt->re = NULL;
       rt->md = NULL;
    }
    return rt;
  }
  void rem_wild( uint32_t h,  NatsStr &subj );

  /* put in any elem, search for it and append if not found
   * sub_tab[ sub ] => sid, sid2...
   * sid_tab[ sid ] => subj */
  NatsSubStatus put( NatsStr &subj,  NatsStr &sid ) {
    NatsSubStatus status = this->put( this->sub_map, subj, sid );
    switch ( status ) {
      case NATS_IS_NEW:
      case NATS_OK:
        this->put( this->sid_map, sid, subj );
        break;
      default:
        break;
    }
    return status;
  }
  /* add sub or sid if it doesn't exist */
  NatsSubStatus put( NatsMap &map,  NatsStr &one,  NatsStr &two ) {
    NatsIter     iter;
    NatsStr      mem;
    NatsMapRec * rt;
    RouteLoc     loc;
    size_t       off, new_len;
    NatsPair     val( one, &two );

    rt = map.upsert( one.hash(), &val, val.length(), loc );
    if ( loc.is_new ) {
      rt->msg_cnt  = 0;
      rt->max_msgs = 0;
      return NATS_IS_NEW;
    }
    /* check if already added */
    iter.init( *rt );
    if ( iter.is_member( two ) )
      return NATS_EXISTS;
    off = rt->len;
    new_len = off + (size_t) two.length();
    if ( new_len > 0xffffU )
      return NATS_TOO_MANY;
    rt = map.resize( one.hash(), &val, off, new_len, loc );
    two.copy( &rt->value[ off ] );
    return NATS_OK;
  }
  /* for a publish, increment msg count and check max-ed expired sids */
  NatsSubStatus lookup_publish( NatsStr &subj,  NatsLookup &pub ) {
    NatsPair val( subj );
    if ( (pub.rt = this->sub_map.find( subj.hash(), &val, 0,
                                       pub.loc )) != NULL ) {
      pub.rt->msg_cnt++;
      pub.iter.init( *pub.rt );
      if ( pub.rt->max_msgs == 0 || pub.rt->max_msgs > pub.rt->msg_cnt )
        return NATS_OK;
      return NATS_EXPIRED;
    }
    return NATS_NOT_FOUND;
  }
  NatsSubStatus lookup_wild( NatsStr &,  NatsLookup & ) {
    return NATS_NOT_FOUND;
  }
  /* after publish, remove sids that are dead */
  NatsSubStatus expire_publish( NatsStr &subj,  NatsLookup &pub ) {
    RouteLoc     loc;
    NatsStr      sid;
    NatsMapRec * rt;
    uint32_t     max_msgs = pub.rt->max_msgs;

    pub.iter.off = subj.length();
    for (;;) {
      if ( ! pub.iter.get( sid ) )
        break;
      NatsPair val( sid );
      rt = this->sid_map.find( sid.hash(), &val, 0, loc );
      /* if the sid max matches the current minimum max, remove it */
      if ( rt->max_msgs == pub.rt->max_msgs ) {
        pub.iter.remove();
        if ( rt->len == sid.length() + subj.length() )
          this->sid_map.remove( loc );
        else {
          NatsIter iter;
          iter.init( *rt );
          /* if sid is a member of subscription */
          if ( iter.is_member( subj ) ) {
            iter.remove();
            uint16_t new_len = rt->len - iter.rmlen;
            this->sid_map.resize( sid.hash(), &val, rt->len, new_len, loc );
          }
        }
      }
      /* calculate the new minimum */
      else {
        if ( rt->max_msgs > pub.rt->max_msgs ) {
          if ( max_msgs == pub.rt->max_msgs || rt->max_msgs < max_msgs )
            max_msgs = rt->max_msgs;
        }
        pub.iter.incr();
      }
    }
    /* if a new max, the sub stays alive */
    if ( max_msgs != pub.rt->max_msgs ) {
      pub.rt->max_msgs = max_msgs;
      if ( pub.iter.rmlen > 0 ) {
        uint16_t new_len = pub.rt->len - pub.iter.rmlen;
        NatsPair val( subj );
        this->sub_map.resize( subj.hash(), &val, pub.rt->len, new_len,
                              pub.loc );
      }
      return NATS_OK;
    }
    /* all sids are dereferenced, sub is removed */
    this->sub_map.remove( pub.loc );
    return NATS_EXPIRED;
  }
  /* unsub sid <max-msgs>, remove or mark sid with max-msgs */
  NatsSubStatus expire_sid( NatsStr &sid,  uint32_t max_msgs ) {
    RouteLoc     loc;
    NatsPair     val( sid );
    NatsMapRec * rt = this->sid_map.find( sid.hash(), &val, 0, loc );
    /* if sid exists */
    if ( rt != NULL ) {
      NatsIter iter;
      NatsStr  subj;
      uint32_t exp_cnt = 0,
               subj_cnt = 0;
      iter.init( *rt );
      /* for each sid, deref the subject */
      while ( iter.get( subj ) ) {
        if ( this->expire_subj( subj, sid, max_msgs ) != NATS_OK ) {
          iter.remove();
          exp_cnt++;
        }
        else {
          iter.incr();
        }
        subj_cnt++;
      }
      /* if all subjects derefed, remove sid */
      if ( max_msgs == 0 || exp_cnt == subj_cnt ) {
        this->sid_map.remove( loc );
        return NATS_EXPIRED;
      }
      /* has a max-msgs that will expire after publishes */
      else {
        rt->max_msgs = max_msgs;
        if ( exp_cnt > 0 ) {
          uint16_t new_len = rt->len - iter.rmlen;
          this->sid_map.resize( sid.hash(), &val, rt->len, new_len, loc );
        }
        return NATS_OK;
      }
    }
    return NATS_NOT_FOUND;
  }
  NatsSubStatus lookup_sid( NatsStr &sid,  NatsLookup &uns ) {
    NatsPair     val( sid );
    uns.rt = this->sid_map.find( sid.hash(), &val, 0, uns.loc );
    /* if sid exists */
    if ( uns.rt == NULL )
      return NATS_NOT_FOUND;
    uns.iter.init( *uns.rt );
    return NATS_OK;
  }
  /* remove subject or mark with max-msgs */
  NatsSubStatus expire_subj( NatsStr &subj,  NatsStr &sid,
                             uint32_t max_msgs ) {
    RouteLoc     loc;
    NatsPair     val( subj );
    NatsMapRec * rt = this->sub_map.find( subj.hash(), &val, 0, loc );
    /* if subject exists */
    if ( rt != NULL ) {
      NatsIter iter;
      iter.init( *rt );
      /* if sid is a member of subscription */
      if ( iter.is_member( sid ) ) {
        /* triggers if max_msgs == 0 (not-defined) or less than msg_cnt */
        if ( max_msgs <= rt->msg_cnt ) {
          uint16_t new_len = rt->len - iter.curlen;
          /* if no more sids left */
          if ( new_len == subj.length() ) {
            this->sub_map.remove( loc );
            return NATS_EXPIRED;
          }
          /* remove sid */
          else {
            NatsPair val( subj );
            iter.remove();
            this->sub_map.resize( subj.hash(), &val, rt->len, new_len, loc );
            return NATS_OK;
          }
        }
        /* max_msgs is greater than the msg_cnt, update the subject */
        else {
          if ( max_msgs != 0 ) {
            if ( rt->max_msgs == 0 || max_msgs < rt->max_msgs )
              rt->max_msgs = max_msgs;
          }
          return NATS_OK;
        }
      }
    }
    return NATS_NOT_FOUND;
  }

  void print( void ) {
    printf( "subj:\n" );
    print_tab( this->sub_map );
    printf( "sid:\n" );
    print_tab( this->sid_map );
  }

  void print_tab( NatsMap &map ) {
    NatsMapRec * rt;
    uint32_t     i;
    uint16_t     off;
    NatsStr      one, two;

    for ( rt = map.first( i, off ); rt != NULL;
          rt = map.next( i, off ) ) {
      one.read( rt->value );
      printf( " [%u.%u,%x", i, off, rt->hash );
      if ( rt->hash != one.hash() )
        printf( ",!h!" );
      RouteLoc loc;
      NatsPair tmp( one );
      if ( map.find( rt->hash, &tmp, 0, loc ) != rt )
        printf( ",!r!" );
      else
        printf( ",%u", loc.j );
      printf( "] cnt=%u,max=%u,%.*s: ", rt->msg_cnt, rt->max_msgs,
              (int) one.len, one.str );
      NatsIter iter;
      iter.init( *rt );
      if ( iter.get( two ) ) {
        printf( "%.*s", (int) two.len, two.str );
        for (;;) {
          iter.incr();
          if ( ! iter.get( two ) )
            break;
          printf( ", %.*s", (int) two.len, two.str );
        }
      }
      printf( "\n" );
    }
  }
};

}
}
#endif
