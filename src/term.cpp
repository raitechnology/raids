#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <raids/term.h>
#include <raids/redis_cmd_db.h>
#include <linecook/linecook.h>
#include <linecook/ttycook.h>

using namespace rai;
using namespace ds;

static int
do_read( LineCook *state,  void *buf,  size_t buflen )
{
  return ((Term *) state->closure)->tty_read( buf, buflen );
}

static int
do_write( LineCook *state,  const void *buf,  size_t buflen )
{
  return ((Term *) state->closure)->tty_write( buf, buflen );
}

static int
do_complete( LineCook *state,  const char * /*buf*/,  size_t off,
             size_t /*len*/ )
{
  if ( off == 0 ) {
    char cmd[ 64 ];
    for ( size_t i = 1; i < cmd_db_cnt; i++ ) {
      size_t len = ::strlen( cmd_db[ i ].name );
      if ( len > 64 ) len = 64;
      for ( size_t j = 0; j < len; j++ )
        cmd[ j ] = tolower( cmd_db[ i ].name[ j ] );
      lc_add_completion( state, cmd, len );
    }
  }
  return 0;
}

void
Term::tty_init( void )
{
  static const char * prompt = /*ANSI_RED     "\\U00002764 " ANSI_NORMAL*/
                               ANSI_CYAN    "ds"    ANSI_NORMAL "@"
                               ANSI_MAGENTA "\\h"   ANSI_NORMAL
                               ANSI_BLUE    "["     ANSI_NORMAL
                               ANSI_RED     "\\#"   ANSI_NORMAL
                               ANSI_BLUE    "]"     ANSI_NORMAL "\\$ ",
                    * promp2 = ANSI_BLUE    "> "    ANSI_NORMAL,
                    * ins    = ANSI_GREEN   "<-"    ANSI_NORMAL,
                    * cmd    = ANSI_MAGENTA "|="    ANSI_NORMAL,
                    * emacs  = ANSI_GREEN   "<e"    ANSI_NORMAL,
                    * srch   = ANSI_CYAN    "/_"    ANSI_NORMAL,
                    * comp   = ANSI_MAGENTA "TAB"   ANSI_NORMAL,
                    * visu   = ANSI_CYAN    "[-]"   ANSI_NORMAL,
                    * ouch   = ANSI_RED     "?@&!" ANSI_NORMAL,
                    * sel1   = ANSI_RED     "["     ANSI_NORMAL,
                    * sel2   = ANSI_RED     "]"     ANSI_NORMAL,
                    * brk    = " \t\n\\'`><=;|&{()}",
                    * qc     = " \t\n\\\"'@<>=;|&()#$`?*[!:{";

  this->lc              = lc_create_state( 120, 50 );
  this->lc->closure     = this;
  this->lc->read_cb     = do_read;
  this->lc->write_cb    = do_write;
  this->lc->complete_cb = do_complete;
  this->tty             = lc_tty_create( this->lc );

  /*lc_tty_set_locale(); */
  lc_set_completion_break( this->tty->lc, brk, strlen( brk ) );
  lc_set_quotables( this->tty->lc, qc, strlen( qc ), '\"' );
  lc_tty_open_history( this->tty, ".console_history" );

  /* init i/o fd, prompt vars, geometry, SIGWINCH */
  lc_tty_set_prompt( this->tty, TTYP_PROMPT1, prompt );
  lc_tty_set_prompt( this->tty, TTYP_PROMPT2, promp2 );
  lc_tty_set_prompt( this->tty, TTYP_R_INS,   ins );
  lc_tty_set_prompt( this->tty, TTYP_R_CMD,   cmd );
  lc_tty_set_prompt( this->tty, TTYP_R_EMACS, emacs );
  lc_tty_set_prompt( this->tty, TTYP_R_SRCH,  srch );
  lc_tty_set_prompt( this->tty, TTYP_R_COMP,  comp );
  lc_tty_set_prompt( this->tty, TTYP_R_VISU,  visu );
  lc_tty_set_prompt( this->tty, TTYP_R_OUCH,  ouch );
  lc_tty_set_prompt( this->tty, TTYP_R_SEL1,  sel1 );
  lc_tty_set_prompt( this->tty, TTYP_R_SEL2,  sel2 );
}

bool
Term::tty_prompt( void )
{
  size_t x = this->out_len;
  lc_tty_get_line( this->tty ); /* output a prompt */
  return x != this->out_len;
}

void
Term::tty_release( void )
{
  if ( this->tty != NULL )
    lc_tty_release( this->tty );     /* free tty */
  if ( this->lc != NULL )
    lc_release_state( this->lc );    /* free lc */
  if ( this->line_buf != NULL )
    ::free( this->line_buf );
  if ( this->out_buf != NULL )
    ::free( this->out_buf );
  this->zero();
}

void
Term::show_help( void )
{
  int  arg_num,   /* which arg is completed, 0 = first */
       arg_count, /* how many args */
       arg_off[ 32 ],  /* offset of args */
       arg_len[ 32 ];  /* length of args */
  char buf[ 1024 ], line[ 80 ];
  int n = lc_tty_get_completion_cmd( this->tty, buf, sizeof( buf ),
                                     &arg_num, &arg_count, arg_off,
                                     arg_len, 32 );
  if ( n <= 0 )
    return;

  for ( size_t i = 1; i < cmd_db_cnt; i++ ) {
    const char * name     = cmd_db[ i ].name;
    size_t       name_len = ::strlen( name );
    if ( (size_t) arg_len[ 0 ] == name_len &&
         ::strncasecmp( name, &buf[ arg_off[ 0 ] ], name_len ) == 0 ) {
      const char * descr = cmd_db[ i ].descr,
                 * ptr   = ::strstr( descr, " ; " );
      size_t len;
      bool first = true;
      for (;;) {
        if ( ptr >= descr )
          len = ptr - descr;
        else
          len = ::strlen( descr );
        if ( len > 0 ) {
          size_t       j,
                       k    = len,
                       off  = 0;
          if ( ptr == NULL )
            j = 3;
          else
            j = name_len;
          if ( ! first || ptr == NULL )
            name = "";
          for (;;) {
            k = len - off;
            if ( k == 0 )
              break;
            if ( j + k > 76 ) {
              for ( k = 76 - j; ; k -= 1 ) {
                if ( descr[ off + k ] == ' ' || descr[ off + k ] == '|' )
                  break;
                if ( k < 20 )
                  break;
              }
            }
            int n = ::snprintf( line, 79, "%*s %.*s", (int) j, name, 
                                (int) k, &descr[ off ] );
            lc_add_completion( this->lc, line, n );
            off += k;
            name = "";
          }
          first = false;
        }
        if ( ptr == NULL )
          break;
        descr = &ptr[ 3 ];
        ptr   = NULL;
      }
      break;
    }
  }
}

void
Term::tty_input( const void *buf,  size_t buflen )
{
  this->in_buf = buf;
  this->in_off = 0;
  this->in_len = buflen;

  while ( this->in_off < this->in_len ||
          this->tty->lc_status == LINE_STATUS_COMPLETE ) {
    if ( lc_tty_get_line( this->tty ) >= 0 ) {
      if ( this->tty->lc_status == LINE_STATUS_INTERRUPT ||
           this->tty->lc_status == LINE_STATUS_SUSPEND ) {
        if ( this->tty->lc_status == LINE_STATUS_INTERRUPT )
          this->interrupt++;
        else
          this->suspend++;
        lc_tty_set_continue( tty, 0 ); /* cancel continue */
        lc_tty_break_history( tty );   /* cancel buffered line */
        continue;
      }
      if ( this->tty->lc_status == LINE_STATUS_COMPLETE ) {
        CompleteType ctype = lc_get_complete_type( lc );
        if ( ctype == COMPLETE_HELP )
          this->show_help();
      }
      if ( this->tty->lc_status == LINE_STATUS_EXEC ) {
        size_t len = this->tty->line_len;
        if ( len > 0 && this->tty->line[ len - 1 ] == '\\' ) {
          lc_tty_set_continue( this->tty, 1 );
          /* push it back, it will be prepended to the next line */
          lc_tty_push_line( this->tty, this->tty->line, len - 1 );
          continue;
        }
        lc_tty_set_continue( this->tty, 0 );
        /* log the line to history file (.console_history) */
        lc_tty_log_history( this->tty );
        len += 2;
        if ( this->line_off > 0 ) {
          this->line_len -= this->line_off;
          if ( this->line_len > 0 )
            ::memmove( this->line_buf, &this->line_buf[ this->line_off ],
                       this->line_len );
          this->line_off = 0;
        }
        if ( this->line_len + len > this->line_buflen ) {
          void *p = ::realloc( this->line_buf, this->line_len + len );
          if ( p == NULL )
            return;
          this->line_buf    = (char *) p;
          this->line_buflen = this->line_len + len;
        }
        ::memcpy( &this->line_buf[ this->line_len ], this->tty->line, len - 2 );
        this->line_len += len;
        this->line_buf[ this->line_len - 2 ] = '\r';
        this->line_buf[ this->line_len - 1 ] = '\n';
      }
    }
  }
}

int
Term::tty_read( void *buf,  size_t buflen )
{
  size_t len = this->in_len - this->in_off;
  if ( len > 0 ) {
    if ( len > buflen )
      len = buflen;
    ::memcpy( buf, &((const char *) this->in_buf)[ this->in_off ], len );
    this->in_off += len;
    return len;
  }
  return 0;
}

int
Term::tty_write( const void *buf, size_t buflen )
{
  if ( buflen + this->out_len > this->out_buflen ) {
    void *p = ::realloc( this->out_buf, buflen + this->out_len );
    if ( p == NULL )
      return -1;
    this->out_buf    = (char *) p;
    this->out_buflen = buflen + this->out_len;
  }
  ::memcpy( &this->out_buf[ this->out_len ], buf, buflen );
  this->out_len += buflen;
  return buflen;
}

