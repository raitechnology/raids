#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <raids/ev_client.h>
#include <linecook/linecook.h>
#include <linecook/ttycook.h>
#include <raids/ev_memcached.h>

using namespace rai;
using namespace ds;

void
EvClient::send_data( char *,  size_t ) noexcept
{
}

void
EvNetClient::send_data( char *data,  size_t size ) noexcept
{
  this->append_iov( data, size );
  this->idle_push( EV_WRITE );
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
      this->off += buflen;
    else
      break;
  }
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
}

void
EvNetClient::process_close( void ) noexcept
{
  this->cb.on_close();
}

void
EvUdpClient::send_data( char *data,  size_t size ) noexcept
{
  if ( ! this->pending() ) {
    MemcachedHdr hdr;
    hdr.req_id = __builtin_bswap16( this->req_id++ );
    hdr.seqno  = 0;
    hdr.total  = __builtin_bswap16( 1 );
    hdr.opaque = 0;
    this->append( &hdr, sizeof( hdr ) );
  }
  this->append_iov( data, size );
  this->idle_push( EV_WRITE );
}

void
EvUdpClient::write( void ) noexcept
{
  StreamBuf & strm = *this;
  uint32_t    out_idx[ 2 ];
  MemcachedUdpFraming g( out_idx, NULL, strm, 1 );
  if ( strm.sz > 0 )
    strm.flush();
  out_idx[ 0 ] = 0;
  out_idx[ 1 ] = strm.idx; /* extent of iov[] array */
  g.construct_frames();
  this->out_nmsgs = g.out_nmsgs;
  this->out_mhdr  = g.out_mhdr;
  this->EvUdp::write();
}

void
EvUdpClient::process( void ) noexcept
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

      total = __builtin_bswap16( h->total );
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
            off += buflen;
          else
            break;
        }
      }
    }
    this->in_moff++; /* next buffer */
  }
  this->pop( EV_PROCESS );
  if ( this->pending() > 0 )
    this->push( EV_WRITE );
}

void
EvUdpClient::process_close( void ) noexcept
{
  this->cb.on_close();
}

void
EvUdpClient::release( void ) noexcept
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

void
EvTerminal::process( void ) noexcept
{
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
  this->flush_out();

  if ( this->line_len > 0 ) {
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

void
EvTerminal::flush_out( void ) noexcept
{
  for ( size_t i = 0;;) {
    if ( i == this->term.out_len ) {
      this->term.tty_out_reset();
      return;
    }
    int n = ::write( STDOUT_FILENO, &this->term.out_buf[ i ],
                     this->term.out_len - i );
    if ( n < 0 ) {
      if ( errno != EAGAIN && errno != EINTR ) {
        this->cb.on_close();
        return;
      }
    }
    else {
      i += (size_t) n;
    }
  }
}

int
EvTerminal::start( void ) noexcept
{
  this->PeerData::init_peer( STDIN_FILENO, NULL, "term" );
  lc_tty_set_locale();
  this->term.tty_init();
  lc_tty_init_fd( this->term.tty, STDIN_FILENO, STDOUT_FILENO );
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
EvTerminal::printf( const char *fmt, ... ) noexcept
{
  va_list args;
  lc_tty_clear_line( this->term.tty );
  this->flush_out();
  lc_tty_normal_mode( this->term.tty );
  va_start( args, fmt );
  vprintf( fmt, args );
  va_end( args );
  fflush( stdout );
  this->term.tty_prompt();
  this->flush_out();
}

