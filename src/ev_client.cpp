#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <sys/socket.h>
#else
#include <raikv/win.h>
#endif
#include <raikv/util.h>
#include <raikv/os_file.h>
#include <raids/ev_client.h>
#include <linecook/linecook.h>
#include <linecook/ttycook.h>
#include <raids/ev_memcached.h>

using namespace rai;
using namespace ds;
using namespace kv;

void
EvClient::send_data( char *,  size_t ) noexcept
{
}

void
EvNetClient::send_data( char *data,  size_t size ) noexcept
{
  this->append_iov( data, size );
  this->idle_push_write();
}

void
EvNetClient::process( void ) noexcept
{
  for (;;) {
    char * buf    = &this->recv[ this->off ];
    size_t buflen = this->len - this->off;
    if ( buflen == 0 )
      break;
    if ( this->cb.on_data( buf, buflen ) )
      this->off += (uint32_t) buflen;
    else
      break;
  }
  this->pop( EV_PROCESS );
  this->push_write();
}

void
EvNetClient::process_close( void ) noexcept
{
  this->cb.on_close();
  this->EvSocket::process_close();
}

void
EvNetClient::release( void ) noexcept
{
  this->EvConnection::release_buffers();
}

void
EvMemcachedUdpClient::send_data( char *data,  size_t size ) noexcept
{
  if ( ! this->pending() ) {
    MemcachedHdr hdr;
    hdr.req_id = kv_bswap16( this->req_id++ );
    hdr.seqno  = 0;
    hdr.total  = kv_bswap16( 1 );
    hdr.opaque = 0;
    this->append( &hdr, sizeof( hdr ) );
  }
  this->append_iov( data, size );
  this->idle_push( EV_WRITE );
}

void
EvMemcachedUdpClient::write( void ) noexcept
{
  StreamBuf & strm = *this;
  uint32_t    out_idx[ 2 ];
  MemcachedUdpFraming g( out_idx, NULL, strm, 1 );
  if ( strm.sz > 0 )
    strm.flush();
  out_idx[ 0 ] = 0;
  out_idx[ 1 ] = (uint32_t) strm.idx; /* extent of iov[] array */
  g.construct_frames();
  this->out_nmsgs = g.out_nmsgs;
  this->out_mhdr  = g.out_mhdr;
  this->EvUdp::write();
}

void
EvMemcachedUdpClient::process( void ) noexcept
{
  StreamBuf & strm = *this;
  /* for each UDP message recvd */
  while ( this->in_moff < this->in_nmsgs ) {
    uint32_t i   = this->in_moff,
             len = this->in_mhdr[ i ].msg_len;

    if ( len > MC_HDR_SIZE ) {
      char         * buf = (char *)
                           this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_base;
      MemcachedHdr * h   = (MemcachedHdr *) (void *) buf;
      uint16_t       total;

      total = kv_bswap16( h->total );
      if ( total != 1 ) {
        if ( this->sav == NULL ) {
          this->sav = new ( ::malloc( sizeof( EvMemcachedMerge ) ) )
            EvMemcachedMerge();
        }
        if ( this->sav->merge_frames( strm, this->in_mhdr, this->in_nmsgs,
                                      h->req_id, i, total, len ) )
          continue; /* retry the same buffer */
        /* otherwise, drop the request for now, could be completed later */
      }
      else {
        size_t buflen;
        for ( uint32_t off = MC_HDR_SIZE; off < len; ) {
          buflen = len - off;
          if ( this->cb.on_data( &buf[ off ], buflen ) )
            off += (uint32_t) buflen;
          else
            break;
        }
      }
    }
    this->in_moff++; /* next buffer */
  }
  this->pop( EV_PROCESS );
  this->push_write();
}

void
EvMemcachedUdpClient::process_close( void ) noexcept
{
  this->cb.on_close();
  this->EvSocket::process_close();
}

void
EvMemcachedUdpClient::release( void ) noexcept
{
  if ( this->sav != NULL ) {
    this->sav->release();
    ::free( this->sav );
    this->sav = NULL;
  }
  this->EvUdp::release_buffers();
}

bool
EvCallback::on_data( char *,  size_t & ) noexcept
{
  return true;
}

void
EvCallback::on_close( void ) noexcept
{
  fprintf( stderr, "closed\n" );
}

EvTerminal::EvTerminal( kv::EvPoll &p,  EvCallback &callback )
  : EvClient( callback ), kv::EvConnection( p, p.register_type( "term" ) ),
    line( 0 ), line_len( 0 ),
    stdin_fd( STDIN_FILENO ), stdout_fd( STDOUT_FILENO )
{
  this->sock_opts = OPT_NO_CLOSE;
}

void
EvTerminal::process_close( void ) noexcept
{
  this->cb.on_close();
  this->EvSocket::process_close();
}

void
EvTerminal::release( void ) noexcept
{
  this->EvConnection::release_buffers();
}

void
EvTerminal::process( void ) noexcept
{
  if ( this->term.out_len > 0 )
    if ( ! this->flush_out() )
      return;
  size_t buflen = this->len - this->off;
  size_t msgcnt = 0;
  int cnt = this->term.interrupt + this->term.suspend;
  this->term.tty_input( &this->recv[ this->off ], buflen );
  this->off = this->len;

  for (;;) {
    buflen = this->term.line_len - this->term.line_off;
    if ( buflen == 0 )
      break;
    char * buf = &this->term.line_buf[ this->term.line_off ];
    if ( this->cb.on_data( buf, buflen ) ) {
      this->term.line_off += buflen;
      msgcnt++;
    }
    else
      break;
  }
  if ( msgcnt > 0 || cnt != this->term.interrupt + this->term.suspend )
    this->term.tty_prompt();
  if ( ! this->flush_out() )
    return;

  if ( this->line_len > 0 ) { /* this is to inject a line not from tty */
    if ( this->cb.on_data( this->line, this->line_len ) ) {
      ::free( this->line );
      this->line = NULL;
      this->line_len = 0;
    }
  }
  this->pop( EV_PROCESS );
}

void
EvTerminal::process_line( const char *s ) noexcept
{
  size_t slen = ::strlen( s );
  this->line = (char *) ::realloc( this->line, this->line_len + slen + 1 );
  if ( this->line != NULL ) {
    ::memcpy( &this->line[ this->line_len ], s, slen );
    this->line_len += slen;
  }
  this->idle_push( EV_PROCESS );
}

bool
EvTerminal::flush_out( void ) noexcept
{
  size_t i = this->term.out_off;
  for (;;) {
    if ( i >= this->term.out_len ) {
      this->term.tty_out_reset();
      return true;
    }
    size_t  left = this->term.out_len - i;
    char  * ptr  = &this->term.out_buf[ i ];
    char  * eol;
    size_t  nl = 0;
    if ( ptr[ 0 ] == '\n' )
      nl = 1;
    else if ( left > 1 && ptr[ 0 ] == '\r' && ptr[ 1 ] == '\n' )
      nl = 2;
    if ( nl > 0 ) {
      static const char crlf[ 2 ] = { '\r', '\n' };
      if ( os_write( this->stdout_fd, crlf, 2 ) < 0 )
        break;
      i    += nl;
      left -= nl;
      ptr  += nl;
    }
    if ( left > 0 ) {
      if ( (eol = (char *) ::memchr( ptr, '\n', left )) != NULL ) {
        left = eol - ptr;
        if ( left > 0 && *(eol - 1) == '\r' )
          left--;
      }
      if ( left > 0 ) {
        ssize_t n = os_write( this->stdout_fd, ptr, left );
        if ( n < 0 )
          break;
        i += (size_t) n;
      }
    }
  }
  if ( ! ev_would_block( errno ) )
    this->cb.on_close();
  this->term.out_off = i;
  this->idle_push( EV_PROCESS );
  return false;
}

int
EvTerminal::start( void ) noexcept
{
  this->PeerData::init_peer( this->poll.get_next_id(), this->stdin_fd,
                             -1, NULL, "term" );
  lc_tty_set_locale();
  this->term.tty_init();
  lc_tty_init_fd( this->term.tty, this->stdin_fd, this->stdout_fd );
  lc_tty_init_geom( this->term.tty );     /* try to determine lines/cols */
  lc_tty_init_sigwinch( this->term.tty ); /* install sigwinch handler */
  this->term.tty_prompt();
  this->flush_out();
  return this->poll.add_sock( this );
}

void
EvTerminal::finish( void ) noexcept
{
  if ( this->term.tty != NULL ) {
    lc_tty_clear_line( this->term.tty );
    this->flush_out();
    lc_tty_normal_mode( this->term.tty );
  }
  this->term.tty_release();
}

void
EvTerminal::output( const char *buf,  size_t buflen ) noexcept
{
  lc_tty_clear_line( this->term.tty );
  this->flush_out();
  lc_tty_normal_mode( this->term.tty );
  this->term.tty_write( buf, buflen );
  this->term.tty_prompt();
  this->flush_out();
}

int
EvTerminal::vprintf( const char *fmt, va_list args ) noexcept
{
  lc_tty_clear_line( this->term.tty );
  this->flush_out();
  lc_tty_normal_mode( this->term.tty );
  size_t amt = 256;
  int n;
  for (;;) {
    size_t avail = this->term.out_buflen - this->term.out_len;

    if ( avail < amt ) {
      void *p = ::realloc( this->term.out_buf, amt + this->term.out_buflen );
      if ( p == NULL )
        return -1;
      this->term.out_buf     = (char *) p;
      this->term.out_buflen += amt;
      avail += amt;
    }
    amt += 1024;
    va_list cpy;
    va_copy( cpy, args );
    if ( (n = ::vsnprintf( &this->term.out_buf[ this->term.out_len ], avail,
                          fmt, cpy )) < (int) avail ) {
      this->term.out_len += n;
      break;
    }
    va_end( cpy );
    if ( n < 0 )
      return -1;
  }
  this->term.tty_prompt();
  this->flush_out();
  return n;
}

int
EvTerminal::printf( const char *fmt, ... ) noexcept
{
  va_list args;
  va_start( args, fmt );
  int n = this->vprintf( fmt, args );
  va_end( args );
  return n;
}

