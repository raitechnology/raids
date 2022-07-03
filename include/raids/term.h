#ifndef __rai_raids__term_h__
#define __rai_raids__term_h__

extern "C" {
typedef struct LineCook_s LineCook;
typedef struct TTYCook_s TTYCook;
struct Term_s {};
typedef void (*show_help_cb)( struct Term_s * );
}

namespace rai {
namespace ds {

struct Term : public Term_s {
  LineCook   * lc;
  TTYCook    * tty;
  char       * out_buf;    /* terminal control output */
  size_t       out_off,
               out_len,
               out_buflen;
  char       * line_buf;   /* line ready output */
  size_t       line_off,
               line_len,
               line_buflen;
  const void * in_buf;     /* line input data */
  size_t       in_off,
               in_len;
  int          interrupt, /* if LINE_STATUS_INTERRUPT */
               suspend;   /* if LINE_STATUS_SUSPEND */
  void       * closure;
  show_help_cb help_cb;
  const char * prompt,
             * prompt2,
             * ins,
             * cmd,
             * emacs,
             * srch,
             * comp,
             * visu,
             * ouch,
             * sel1,
             * sel2,
             * brk,
             * qc,
             * history;
  Term() {
    this->zero();
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }
  void tty_init( void ) noexcept;
  void tty_out_reset( void ) { this->out_off = this->out_len = 0; }
  void tty_release( void ) noexcept;
  void show_help( void ) noexcept;
  void tty_input( const void *data,  size_t data_len ) noexcept;
  int tty_read( void *buf,  size_t buflen ) noexcept;
  int tty_write( const void *buf,  size_t buflen ) noexcept;
  bool tty_prompt( void ) noexcept;
};

}
}
#endif
