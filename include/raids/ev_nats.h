#ifndef __rai_raids__ev_nats_h__
#define __rai_raids__ev_nats_h__

#include <raids/ev_tcp.h>
#include <raids/nats_map.h>

namespace rai {
namespace ds {

struct EvNatsListen : public EvTcpListen {
  EvNatsListen( EvPoll &p ) : EvTcpListen( p ) {}
  virtual void accept( void );
};

struct EvPrefetchQueue;

enum NatsState {
  NATS_HDR_STATE = 0, /* parsing header opcode */
  NATS_PUB_STATE = 1  /* parsing PUB message bytes */
};

struct EvNatsService : public EvConnection {
  void * operator new( size_t, void *ptr ) { return ptr; }
  NatsSubMap map;

  char     * msg_ptr;     /* ptr to the msg blob */
  size_t     msg_len;     /* size of the current message blob */
  NatsState  msg_state;   /* ascii hdr mode or binary blob mode */
  bool       verbose,     /* whether +OK is sent */
             pedantic,    /* whether subject is checked for validity */
             tls_require, /* whether need TLS layer */
             echo;        /* whether to forward pubs to subs owned by client
                             (eg. multicast loopback) */
  char     * subject;     /* either pub or sub subject w/opt reply: */
  size_t     subject_len; /* PUB <subject> <reply> */
  char     * reply;       /* SUB <subject> <sid> */
  size_t     reply_len;   /* len of reply */
  char     * sid;         /* <sid> of SUB */
  size_t     sid_len;     /* len of sid */
  char     * msg_len_ptr;    /* ptr to msg_len ascii */
  size_t     msg_len_digits; /* number of digits in msg_len */
  size_t     tmp_size;       /* amount of buffer[] free */
  char       buffer[ 1024 ]; /* ptrs below index into this space */

  int        protocol;    /* == 1 */
  char     * name,
           * lang,
           * version,
           * user,
           * pass,
           * auth_token;

  EvNatsService( EvPoll &p ) : EvConnection( p, EV_NATS_SOCK ) {}
  void initialize_state( void ) {
    this->msg_ptr   = NULL;
    this->msg_len   = 0;
    this->msg_state = NATS_HDR_STATE;

    this->verbose = true;
    this->pedantic = this->tls_require = false;
    this->echo = true;

    this->subject = this->reply = this->sid = this->msg_len_ptr = NULL;
    this->subject_len = this->reply_len = this->sid_len =
    this->msg_len_digits = 0;
    this->tmp_size = sizeof( this->buffer );

    this->protocol = 1;
    this->name = this->lang = this->version = NULL;
    this->user = this->pass = this->auth_token = NULL;
  }
  void process( bool use_prefetch );
  /*HashData * resize_tab( HashData *curr,  size_t add_len );*/
  void add_sub( void );
  void add_wild( NatsStr &xsubj );
  void rem_sid( uint32_t max_msgs );
  void rem_wild( NatsStr &xsubj );
  void rem_all_sub( void );
  bool fwd_pub( void );
  bool publish( EvPublish &pub );
  bool hash_to_sub( uint32_t h,  char *key,  size_t &keylen );
  bool fwd_msg( EvPublish &pub,  const void *sid,  size_t sid_len );
  void parse_connect( const char *buf,  size_t sz );
  void process_close( void ) {}
  void release( void );
  void push_free_list( void );
  void pop_free_list( void );
};

/* presumes little endian, 0xdf masks out 0x20 for toupper() */
#define NATS_KW( c1, c2, c3, c4 ) ( (uint32_t) ( c4 & 0xdf ) << 24 ) | \
                                  ( (uint32_t) ( c3 & 0xdf ) << 16 ) | \
                                  ( (uint32_t) ( c2 & 0xdf ) << 8 ) | \
                                  ( (uint32_t) ( c1 & 0xdf ) )
#define NATS_KW_OK1     NATS_KW( '+', 'O', 'K', '\r' )
#define NATS_KW_OK2     NATS_KW( '+', 'O', 'K', '\n' )
#define NATS_KW_MSG1    NATS_KW( 'M', 'S', 'G', ' ' )
#define NATS_KW_MSG2    NATS_KW( 'M', 'S', 'G', '\t' )
#define NATS_KW_ERR     NATS_KW( '-', 'E', 'R', 'R' )
#define NATS_KW_SUB1    NATS_KW( 'S', 'U', 'B', ' ' )
#define NATS_KW_SUB2    NATS_KW( 'S', 'U', 'B', '\t' )
#define NATS_KW_PUB1    NATS_KW( 'P', 'U', 'B', ' ' )
#define NATS_KW_PUB2    NATS_KW( 'P', 'U', 'B', '\t' )
#define NATS_KW_PING    NATS_KW( 'P', 'I', 'N', 'G' )
#define NATS_KW_PONG    NATS_KW( 'P', 'O', 'N', 'G' )
#define NATS_KW_INFO    NATS_KW( 'I', 'N', 'F', 'O' )
#define NATS_KW_UNSUB   NATS_KW( 'U', 'N', 'S', 'U' )
#define NATS_KW_CONNECT NATS_KW( 'C', 'O', 'N', 'N' )

/* SUB1  = 4347219, 0x425553;  SUB2 = 155342163, 0x9425553 */
/* PUB1  = 4347216, 0x425550;  PUB2 = 155342160, 0x9425550 */
/* PING  = 1196312912, 0x474e4950 */
/* UNSUB = 1431522901, 0x55534e55 */

#define NATS_JS_VERBOSE     NATS_KW( 'V', 'E', 'R', 'B' )
#define NATS_JS_PEDANTIC    NATS_KW( 'P', 'E', 'D', 'A' )
#define NATS_JS_TLS_REQUIRE NATS_KW( 'T', 'L', 'S', '_' )
#define NATS_JS_NAME        NATS_KW( 'N', 'A', 'M', 'E' )
#define NATS_JS_LANG        NATS_KW( 'L', 'A', 'N', 'G' )
#define NATS_JS_VERSION     NATS_KW( 'V', 'E', 'R', 'S' )
#define NATS_JS_PROTOCOL    NATS_KW( 'P', 'R', 'O', 'T' )
#define NATS_JS_ECHO        NATS_KW( 'E', 'C', 'H', 'O' )
#define NATS_JS_USER        NATS_KW( 'U', 'S', 'E', 'R' )
#define NATS_JS_PASS        NATS_KW( 'P', 'A', 'S', 'S' )
#define NATS_JS_AUTH_TOKEN  NATS_KW( 'A', 'U', 'T', 'H' )

}
}
#endif
