#ifndef __rai_raids__term_h__
#define __rai_raids__term_h__

extern "C" {
typedef struct LineCook_s LineCook;
typedef struct TTYCook_s TTYCook;
}

namespace rai {
namespace ds {

struct Term {
  LineCook   * lc;
  TTYCook    * tty;
  char       * out_buf;    /* terminal control output */
  size_t       out_len,
               out_buflen;
  char       * line_buf;   /* line ready output */
  size_t       line_off,
               line_len,
               line_buflen;
  const void * in_buf;     /* line input data */
  size_t       in_off,
               in_len;

  Term() {
    this->zero();
  }
  void zero( void ) {
    ::memset( this, 0, sizeof( *this ) );
  }

  void tty_init( void );
  void tty_out_reset( void ) { this->out_len = 0; }
  void tty_release( void );
  void tty_input( const void *data,  size_t data_len );
  int tty_read( void *buf,  size_t buflen );
  int tty_write( const void *buf,  size_t buflen );
  bool tty_prompt( void );
};

}
}
#endif
