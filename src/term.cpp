#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <raids/term.h>
#include <raids/redis_cmd.h>
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
    for ( size_t i = 1; i < REDIS_CMD_DB_SIZE; i++ )
      lc_add_completion( state, cmd_db[ i ].name, cmd_db[ i ].cmdlen );
  }
  return 0;
}

void
Term::tty_init( void ) noexcept
{
  if ( this->prompt == NULL )
    this->prompt = ANSI_CYAN    "ds"    ANSI_NORMAL "@"
                   ANSI_MAGENTA "\\h"   ANSI_NORMAL
                   ANSI_BLUE    "["     ANSI_NORMAL
                   ANSI_RED     "\\#"   ANSI_NORMAL
                   ANSI_BLUE    "]"     ANSI_NORMAL "> ";
  if ( this->prompt2 == NULL )
    this->prompt2= ANSI_BLUE    "> "    ANSI_NORMAL;
  if ( this->ins == NULL )
    this->ins    = ANSI_GREEN   "<-"    ANSI_NORMAL;
  if ( this->cmd == NULL )
    this->cmd    = ANSI_MAGENTA "|="    ANSI_NORMAL;
  if ( this->emacs == NULL )
    this->emacs  = ANSI_GREEN   "<e"    ANSI_NORMAL;
  if ( this->srch == NULL )
    this->srch   = ANSI_CYAN    "/_"    ANSI_NORMAL;
  if ( this->comp == NULL )
    this->comp   = ANSI_MAGENTA "TAB"   ANSI_NORMAL;
  if ( this->visu == NULL )
    this->visu   = ANSI_CYAN    "[-]"   ANSI_NORMAL;
  if ( this->ouch == NULL )
    this->ouch   = ANSI_RED     "?@&!"  ANSI_NORMAL;
  if ( this->sel1 == NULL )
    this->sel1   = ANSI_RED     "["     ANSI_NORMAL;
  if ( this->sel2 == NULL )
    this->sel2   = ANSI_RED     "]"     ANSI_NORMAL;
  if ( this->brk == NULL )
    this->brk    = " \t\n\\'`><=;|&{()}";
  if ( this->qc == NULL )
    this->qc     = " \t\n\\\"'@<>=;|&()#$`?*[!:{";
  if ( this->history == NULL )
    this->history = ".console_history";

  this->lc              = lc_create_state( 120, 50 );
  this->lc->closure     = this;
  this->lc->read_cb     = do_read;
  this->lc->write_cb    = do_write;
  this->lc->complete_cb = do_complete;
  this->tty             = lc_tty_create( this->lc );

  /*lc_tty_set_locale(); */
  lc_set_completion_break( this->tty->lc, this->brk, strlen( this->brk ) );
  lc_set_quotables( this->tty->lc, this->qc, strlen( this->qc ), '\"' );
  lc_tty_open_history( this->tty, this->history );

  /* init i/o fd, prompt vars, geometry, SIGWINCH */
  lc_tty_set_prompt( this->tty, TTYP_PROMPT1, this->prompt );
  lc_tty_set_prompt( this->tty, TTYP_PROMPT2, this->prompt2 );
  lc_tty_set_prompt( this->tty, TTYP_R_INS,   this->ins );
  lc_tty_set_prompt( this->tty, TTYP_R_CMD,   this->cmd );
  lc_tty_set_prompt( this->tty, TTYP_R_EMACS, this->emacs );
  lc_tty_set_prompt( this->tty, TTYP_R_SRCH,  this->srch );
  lc_tty_set_prompt( this->tty, TTYP_R_COMP,  this->comp );
  lc_tty_set_prompt( this->tty, TTYP_R_VISU,  this->visu );
  lc_tty_set_prompt( this->tty, TTYP_R_OUCH,  this->ouch );
  lc_tty_set_prompt( this->tty, TTYP_R_SEL1,  this->sel1 );
  lc_tty_set_prompt( this->tty, TTYP_R_SEL2,  this->sel2 );
}

bool
Term::tty_prompt( void ) noexcept
{
  size_t x = this->out_len;
  lc_tty_get_line( this->tty ); /* output a prompt */
  return x != this->out_len;
}

void
Term::tty_release( void ) noexcept
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
Term::show_help( void ) noexcept
{
  int  arg_num,   /* which arg is completed, 0 = first */
       arg_count, /* how many args */
       arg_off[ 32 ],  /* offset of args */
       arg_len[ 32 ];  /* length of args */
  char buf[ 1024 ];
  int n = lc_tty_get_completion_cmd( this->tty, buf, sizeof( buf ),
                                     &arg_num, &arg_count, arg_off,
                                     arg_len, 32 );
  if ( n <= 0 )
    return;

  for ( size_t i = 1; i < REDIS_CMD_DB_SIZE; i++ ) {
    const char * name     = cmd_db[ i ].name;
    size_t       name_len = cmd_db[ i ].cmdlen;
    if ( (size_t) arg_len[ 0 ] == name_len &&
         ::strncasecmp( name, &buf[ arg_off[ 0 ] ], name_len ) == 0 ) {
      const RedisCmdExtra * ex = cmd_db[ i ].get_extra( XTRA_USAGE );
      const char   usage[]   = "\033[35m" "Usage:" ANSI_NORMAL,
                   example[] = "\033[35m" "Example:" ANSI_NORMAL,
                   descr[]   = "\033[35m" "Description:" ANSI_NORMAL,
                   returns[] = "\033[35m" "Return:" ANSI_NORMAL;
      for ( ; ex != NULL; ex = ex->next ) {
        const char * ptr = ex->text,
                   * eol;
        const char * s;
        size_t       len;
        switch ( ex->type ) {
          case XTRA_USAGE:   s = usage;   len = sizeof( usage ) - 1;   break;
          case XTRA_EXAMPLE: s = example; len = sizeof( example ) - 1; break;
          case XTRA_DESCR:   s = descr;   len = sizeof( descr ) - 1;   break;
          case XTRA_RETURN:  s = returns; len = sizeof( returns ) - 1; break;
          default:           s = NULL; len = 0; break;
        }
        if ( s == NULL )
          break;
        lc_add_completion( this->lc, s, len );
        while ( (eol = ::strchr( ptr, '\n' )) != NULL ) {
          lc_add_completion( this->lc, ptr, eol - ptr );
          ptr = &eol[ 1 ];
        }
      }
      break;
    }
  }
}

void
Term::tty_input( const void *buf,  size_t buflen ) noexcept
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
        if ( ctype == COMPLETE_HELP ) {
          if ( this->help_cb != NULL )
            this->help_cb( this );
          else
            this->show_help();
        }
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
Term::tty_read( void *buf,  size_t buflen ) noexcept
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
Term::tty_write( const void *buf, size_t buflen ) noexcept
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
