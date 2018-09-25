#ifndef __rai_raids__ev_publish_h__
#define __rai_raids__ev_publish_h__

namespace rai {
namespace ds {

struct EvPublish {
  const char * subject;
  const void * reply,
             * msg;
  size_t       subject_len,
               reply_len,
               msg_len;
  uint32_t   * routes,
               rcount,
               subj_hash;
  const char * msg_len_buf;
  uint8_t      msg_len_digits;
  uint32_t     src_route;

  EvPublish( const char *subj,  size_t subj_len,
             const void *repl,  size_t repl_len,
             const void *mesg,  size_t mesg_len,
             uint32_t *rtes,    uint32_t rcnt,
             uint32_t src,  uint32_t hash,
             const char *msg_len_ptr,
             uint8_t msg_len_digs )
    : subject( subj ), reply( repl ), msg( mesg ),
      subject_len( subj_len ), reply_len( repl_len ),
      msg_len( mesg_len ), routes( rtes ), rcount( rcnt ),
      subj_hash( hash ), msg_len_buf( msg_len_ptr ),
      msg_len_digits( msg_len_digs ),
      src_route( src ) {}
};

}
}
#endif