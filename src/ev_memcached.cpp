#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <raids/ev_memcached.h>

using namespace rai;
using namespace ds;
using namespace kv;

static MemcachedStats stat;

EvMemcachedListen::EvMemcachedListen( EvPoll &p ) noexcept
                 : EvTcpListen( p, this->ops )
{
  if ( stat.boot_time == 0 )
    stat.boot_time = kv_current_realtime_ns();
}

int
EvMemcachedListen::listen( const char *ip,  int port ) noexcept
{
  if ( ip != NULL )
    ::strncpy( stat.interface, ip, sizeof( stat.interface ) - 1 );
  stat.tcpport = port;
  stat.max_connections = this->poll.nfds;
  return this->EvTcpListen::listen( ip, port, "memcached_listen" );
}

int
EvMemcachedUdp::listen( const char *ip,  int port ) noexcept
{
  if ( ip != NULL )
    ::strncpy( stat.interface, ip, sizeof( stat.interface ) - 1 );
  stat.udpport = port;
  stat.max_connections = this->poll.nfds;
  return this->EvUdp::listen( ip, port, "memcached_udp" );
}

bool
EvMemcachedListen::accept( void ) noexcept
{
  static int on = 1;
  struct sockaddr_storage addr;
  socklen_t addrlen = sizeof( addr );
  int sock = ::accept( this->fd, (struct sockaddr *) &addr, &addrlen );
  if ( sock < 0 ) {
    if ( errno != EINTR ) {
      if ( errno != EAGAIN )
	perror( "accept" );
      this->pop3( EV_READ, EV_READ_LO, EV_READ_HI );
    }
    return false;
  }
  bool was_empty = this->poll.free_memcached.is_empty();
  EvMemcachedService * c =
    this->poll.get_free_list2<EvMemcachedService, MemcachedStats>(
      this->poll.free_memcached, stat );
  if ( c == NULL ) {
    perror( "accept: no memory" );
    ::close( sock );
    return false;
  }
  if ( was_empty )
    stat.conn_structs += EvPoll::ALLOC_INCR;
  struct linger lin;
  lin.l_onoff  = 1;
  lin.l_linger = 10; /* 10 secs */
  if ( ::setsockopt( sock, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof( on ) ) != 0 )
    perror( "warning: SO_KEEPALIVE" );
  if ( ::setsockopt( sock, SOL_SOCKET, SO_LINGER, &lin, sizeof( lin ) ) != 0 )
    perror( "warning: SO_LINGER" );
  if ( ::setsockopt( sock, SOL_TCP, TCP_NODELAY, &on, sizeof( on ) ) != 0 )
    perror( "warning: TCP_NODELAY" );

  ::fcntl( sock, F_SETFL, O_NONBLOCK | ::fcntl( sock, F_GETFL ) );
  c->PeerData::init_peer( sock, (struct sockaddr *) &addr, "memcached" );
  if ( this->poll.add_sock( c ) < 0 ) {
    ::close( sock );
    c->push_free_list();
    return false;
  }
  stat.curr_connections++;
  stat.total_connections++;
  if ( stat.curr_connections > stat.max_connections )
    stat.max_connections = stat.curr_connections;
  return true;
}

void
EvMemcachedUdp::init( void ) noexcept
{
  if ( stat.boot_time == 0 )
    stat.boot_time = kv_current_realtime_ns();
  this->exec = new ( this->execbuf )
    MemcachedExec( *this->poll.map, this->poll.ctx_id, *this, stat );
}

const char *
rai::ds::memcached_status_string( MemcachedStatus status ) noexcept
{
  switch ( status ) {
    case MEMCACHED_OK: return "OK";
    case MEMCACHED_MSG_PARTIAL: return "MSG_PARTIAL";
    case MEMCACHED_EMPTY: return "EMPTY";
    case MEMCACHED_SETUP_OK: return "SETUP_OK";
    case MEMCACHED_SUCCESS: return "SUCCESS";
    case MEMCACHED_DEPENDS: return "DEPENDS";
    case MEMCACHED_CONTINUE: return "CONTINUE";
    case MEMCACHED_QUIT: return "QUIT";
    case MEMCACHED_VERSION: return "VERSION";
    case MEMCACHED_ALLOC_FAIL: return "ALLOC_FAIL";
    case MEMCACHED_BAD_CMD: return "BAD_CMD";
    case MEMCACHED_BAD_ARGS: return "BAD_ARGS";
    case MEMCACHED_INT_OVERFLOW: return "INT_OVERFLOW";
    case MEMCACHED_BAD_INT: return "BAD_INT";
    case MEMCACHED_BAD_INCR: return "BAD_INCR";
    case MEMCACHED_ERR_KV: return "ERR_Kv";
    case MEMCACHED_BAD_TYPE: return "BAD_TYPE";
    case MEMCACHED_NOT_IMPL: return "NOT_IMPL";
    case MEMCACHED_BAD_PAD: return "BAD_PAD";
    case MEMCACHED_BAD_BIN_ARGS: return "BAD_BIN_ARGS";
    case MEMCACHED_BAD_BIN_CMD: return "BAD_BIN_CMD";
    case MEMCACHED_BAD_RESULT: return "BAD_RESULT";
  }
  return "Unknown";
}

const char *
rai::ds::memcached_status_description( MemcachedStatus status ) noexcept
{
  switch ( status ) {
    case MEMCACHED_OK: return "OK";
    case MEMCACHED_MSG_PARTIAL: return "Partial value";
    case MEMCACHED_EMPTY: return "Empty value";
    case MEMCACHED_SETUP_OK: return "Setup OK";
    case MEMCACHED_SUCCESS: return "Success";
    case MEMCACHED_DEPENDS: return "Depends";
    case MEMCACHED_CONTINUE: return "Continue";
    case MEMCACHED_QUIT: return "Quit";
    case MEMCACHED_VERSION: return "Version";
    case MEMCACHED_ALLOC_FAIL: return "Allocation failed";
    case MEMCACHED_BAD_CMD: return "Bad command";
    case MEMCACHED_BAD_ARGS: return "Bad arguments";
    case MEMCACHED_INT_OVERFLOW: return "Integer overflow";
    case MEMCACHED_BAD_INT: return "Bad integer";
    case MEMCACHED_BAD_INCR: return "Bad increment of non integer";
    case MEMCACHED_ERR_KV: return "Err KV";
    case MEMCACHED_BAD_TYPE: return "Bad type";
    case MEMCACHED_NOT_IMPL: return "Not implemented";
    case MEMCACHED_BAD_PAD: return "Bad pad";
    case MEMCACHED_BAD_BIN_ARGS: return "Bad binary args";
    case MEMCACHED_BAD_BIN_CMD: return "Bad binary cmd";
    case MEMCACHED_BAD_RESULT: return "Bad result";
  }
  return "Unknown";
}

inline int
EvMemcached::process_loop( MemcachedExec &mex,  EvPrefetchQueue *q,
                           StreamBuf &strm,  EvSocket *svc ) noexcept
{
  static const char   error[]   = "ERROR";
  static const size_t error_len = sizeof( error ) - 1;
  size_t          buflen;
  MemcachedStatus mstatus,
                  status;

  for (;;) {
    buflen = this->len - this->off;
    if ( buflen == 0 )
      break;
    mstatus = mex.unpack( &this->recv[ this->off ], buflen );
    if ( mstatus != MEMCACHED_OK ) {
      if ( mstatus != MEMCACHED_MSG_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, memcached_status_string( mstatus ), buflen );
        this->off = this->len;
        strm.sz += mex.send_string( error, error_len );
      }
      break;
    }
    this->off += buflen;

    if ( (status = mex.exec( svc, q )) == MEMCACHED_OK )
      if ( strm.alloc_fail )
        status = MEMCACHED_ALLOC_FAIL;
    switch ( status ) {
      case MEMCACHED_SETUP_OK:
        if ( q != NULL )
          return EV_PREFETCH;
        mex.exec_run_to_completion();
        if ( ! strm.alloc_fail )
          break;
        status = MEMCACHED_ALLOC_FAIL;
        /* FALLTHRU */
      default:
        mex.send_err( status );
        break;
      case MEMCACHED_QUIT:
        return EV_SHUTDOWN;
    }
  }
  return EV_WRITE;
}

void
EvMemcachedService::process( void ) noexcept
{
  StreamBuf       & strm = *this;
  EvPrefetchQueue * q    = this->poll.prefetch_queue;
  EvMemcached       evm( this->recv, this->off, this->len );

  switch ( evm.process_loop( *this, q, strm, this ) ) {
    case EV_PREFETCH:
      this->pushpop( EV_PREFETCH, EV_PROCESS ); /* prefetch keys */
      break;
    case EV_SHUTDOWN:
      this->push( EV_SHUTDOWN );
      /* FALLTHRU */
    case EV_WRITE:
      this->pop( EV_PROCESS );
      if ( strm.pending() > 0 )
        this->push( EV_WRITE );
      break;
  }
}

bool
EvMemcachedMerge::merge_frames( StreamBuf &strm,  struct mmsghdr *mhdr,
                                uint32_t nmsgs,  uint32_t req_id,  uint32_t idx,
                                uint32_t total,  uint32_t size ) noexcept
{
  MemcachedHdr * h;
  uint32_t       j, cnt = 1, len;
  uint16_t       seqno;

  for ( j = idx + 1; j < nmsgs; j++ ) {
    len = mhdr[ j ].msg_len;
    if ( len > MC_HDR_SIZE ) {
      h = (MemcachedHdr *) (void *)
          mhdr[ j ].msg_hdr.msg_iov[ 0 ].iov_base;
      /* count the number of frames with the same request id */
      if ( h->req_id == req_id ) {
        cnt++;
        size += len - MC_HDR_SIZE;
      }
    }
  }
  if ( cnt < total ) {
    for ( j = 0; j < this->sav_len; j++ ) {
      len = this->sav_mhdr[ j ].iov_len;
      if ( len > MC_HDR_SIZE ) {
        h = (MemcachedHdr *) (void *) this->sav_mhdr[ j ].iov_base;
        if ( h->req_id == req_id ) {
          cnt++;
          size += len - MC_HDR_SIZE;
        }
      }
    }
  }
  /* count will most likely will refer to each unique seqno in the total,
   * but UDP could duplicate or drop frames */
  if ( cnt >= total ) {
    char * buf = (char *) strm.alloc_temp( size ),
         * ptr = buf,
         * end = &buf[ size ];

    for ( seqno = 0; seqno < total; ) {
      /* concatenate total UDP packets together into a single frame */
      h = NULL;
      for ( j = idx; j < nmsgs; j++ ) {
        len = mhdr[ j ].msg_len;
        if ( len > MC_HDR_SIZE ) {
          h = (MemcachedHdr *) (void *)
              mhdr[ j ].msg_hdr.msg_iov[ 0 ].iov_base;
          /* if the next seqno */
          if ( h->req_id == req_id ) {
            uint16_t h_seqno = __builtin_bswap16( h->seqno );
            if ( h_seqno <= seqno ) {
              mhdr[ j ].msg_len = 0;
              if ( h_seqno == seqno ) /* it's the correct seqno */
                break;
            }
          }
        }
        h = NULL;
      }
      /* check if saved */
      if ( h == NULL && seqno < this->sav_len ) {
        len = this->sav_mhdr[ seqno ].iov_len;
        if ( len > MC_HDR_SIZE ) {
          h = (MemcachedHdr *) (void *) this->sav_mhdr[ seqno ].iov_base;
          if ( h->req_id != req_id || /* probably don't need seqno test */
               h->seqno  != __builtin_bswap16( seqno ) )
            h = NULL;
        }
      }
      if ( h != NULL ) {
        if ( ptr == buf ) {
          if ( &ptr[ len ] > end ) /* reassembly failed */
            break;
          ::memcpy( ptr, h, len );
          h = (MemcachedHdr *) (void *) ptr;
          h->seqno = 0;
          h->total = __builtin_bswap16( 1 );
          ptr = &ptr[ len ];
        }
        else { /* toss the rest of the headers, only need the first */
          if ( &ptr[ len - MC_HDR_SIZE ] > end )
            break;
          ::memcpy( ptr, &h[ 1 ], len - MC_HDR_SIZE );
          ptr = &ptr[ len - MC_HDR_SIZE ];
        }
        mhdr[ j ].msg_len = 0;
        /* all seqnos concatenated */
        if ( ++seqno == total && ptr == end ) {
          mhdr[ idx ].msg_len = size;
          mhdr[ idx ].msg_hdr.msg_iov[ 0 ].iov_base = (void *) buf;
          return true;
        }
      }
    }
  }
  /* failed to reconstruct UDP frames, save for later
   * only one message sequence is saved at a time, so multiple messages with
   * missing frames could drop some requests */

  /* release the old elements, if sav_len > total */
  for ( j = total; j < this->sav_len; j++ ) {
    if ( this->sav_mhdr[ j ].iov_base != NULL ) {
      ::free( this->sav_mhdr[ j ].iov_base );
      this->sav_mhdr[ j ].iov_base = NULL;
      this->sav_mhdr[ j ].iov_len  = 0;
    }
  }
  /* realloc the array to fit all seqnos in msg */
  void *p = ::realloc( this->sav_mhdr, sizeof( this->sav_mhdr[ 0 ] ) * total );
  if ( p == NULL )
    return false;
  this->sav_mhdr = (struct iovec *) p;
  /* null the new elements, if total > sav_len */
  for ( j = this->sav_len; j < total; j++ ) {
    this->sav_mhdr[ j ].iov_base = NULL;
    this->sav_mhdr[ j ].iov_len  = 0;
  }
  this->sav_len = total;
  /* copy the request into sav_mhdr, in case UDP frame arrives later */
  for ( j = idx; j < nmsgs; j++ ) {
    len = mhdr[ j ].msg_len;
    if ( len > MC_HDR_SIZE ) {
      h = (MemcachedHdr *) (void *)
          mhdr[ j ].msg_hdr.msg_iov[ 0 ].iov_base;
      seqno = __builtin_bswap16( h->seqno );
      /* save the frame at seqno index */
      if ( h->req_id == req_id ) {
        mhdr[ j ].msg_len = 0;
        if ( seqno < total ) {
          p = ::realloc( this->sav_mhdr[ seqno ].iov_base, len );
          if ( p != NULL ) {
            ::memcpy( p, h, len );
            this->sav_mhdr[ seqno ].iov_base = p;
            this->sav_mhdr[ seqno ].iov_len  = len;
          }
        }
      }
    }
  }

  return false;
}

void
EvMemcachedUdp::process( void ) noexcept
{
  EvPrefetchQueue * q    = this->poll.prefetch_queue;
  StreamBuf       & strm = *this;
  /* alloc index for tracking iov[] indexes in strm above */
  if ( this->out_idx == NULL )
    this->out_idx = (uint32_t *)
      strm.alloc_temp( sizeof( uint32_t ) * ( this->in_nmsgs + 1 ) );
  /* for each UDP message recvd */
  while ( this->in_moff < this->in_nmsgs ) {
    uint32_t i   = this->in_moff,
             off = MC_HDR_SIZE,
             len = this->in_mhdr[ i ].msg_len;

    if ( strm.sz > 0 )
      strm.flush();
    this->out_idx[ i ] = strm.idx;
    if ( len > MC_HDR_SIZE ) {
      char         * buf = (char *)
                           this->in_mhdr[ i ].msg_hdr.msg_iov[ 0 ].iov_base;
      MemcachedHdr * h   = (MemcachedHdr *) (void *) buf;
      uint16_t       total;

      total = __builtin_bswap16( h->total );
      if ( total != 1 ) {
        if ( this->sav.merge_frames( strm, this->in_mhdr, this->in_nmsgs,
                                     h->req_id, i, total, len ) )
          continue; /* retry the same buffer */
        /* otherwise, drop the request for now, could be completed later */
      }
      else {
        EvMemcached evm( buf, off, len );
        h = (MemcachedHdr *) (void *) strm.alloc( MC_HDR_SIZE );
        strm.sz += MC_HDR_SIZE;
        ::memcpy( h, buf, MC_HDR_SIZE );
        h->seqno = 0;
        h->total = __builtin_bswap16( 1 ); /* adjust later if more than 1 */

        switch ( evm.process_loop( *this->exec, q, strm, this ) ) {
          case EV_PREFETCH:
            this->pushpop( EV_PREFETCH, EV_PROCESS ); /* prefetch keys */
            return;
          case EV_SHUTDOWN: /* no shutdown on udp connections */
            /*this->push( EV_SHUTDOWN );*/
            /* FALLTHRU */
          case EV_WRITE:
            break;
        }
      }
    }
    this->in_moff++; /* next buffer */
  }
  this->pop( EV_PROCESS );
  if ( strm.pending() > 0 )
    this->push( EV_WRITE );
}

bool
MemcachedUdpFraming::construct_frames( void ) noexcept
{
  uint32_t i, o, j, k, off, len, next;
  bool     need_split = false;

  /* calculate if necessary to split the outgoing udp frames,
   * each should be at 1400 bytes (frame_size) until the last in a request */
  for ( i = 0; i < this->nmsgs; ) {
    k = this->out_idx[ i++ ];
    j = this->out_idx[ i ];
    if ( k != j ) {
      next = this->frame_size;
      len  = 0;
      off  = 0;
      this->out_nmsgs++;
      do {
        /* a new header for each udp frame belonging to the same request */
        if ( len == 0 && next == this->frame_size - MC_HDR_SIZE ) {
          this->out_nmsgs++;
          this->iov_cnt++;
          need_split = true;
        }
        /* a smaller iov[] segment packed into a larger udp frame */
        if ( this->strm.iov[ k ].iov_len - off + len < next ) {
          this->iov_cnt++;
          len += this->strm.iov[ k ].iov_len - off;
          off = 0;
          k++;
        }
        /* a larger iov[] segment split into a smaller udp frame */
        else { /* iov[ k ].iov_len - off + len >= next */
          off += next - len; /* off indexes iov_base */
          this->iov_cnt++;
          if ( this->strm.iov[ k ].iov_len == off ) {
            k++;
            off = 0;
          }
          len = 0;
          next = this->frame_size - MC_HDR_SIZE; /* 8 bytes needed for the hdr*/
        }
      } while ( k < j );
    }
  }
  this->out_mhdr = (struct mmsghdr *) (void *)
    strm.alloc_temp( this->out_nmsgs * sizeof( struct mmsghdr ) );
  if ( this->out_mhdr == NULL )
    return false;
  if ( ! need_split ) {
    for ( i = 0, o = 0; i < this->nmsgs; i++ ) {
      k = this->out_idx[ i ],
      j = this->out_idx[ i + 1 ];
      if ( k != j ) {
        struct mmsghdr & oh = this->out_mhdr[ o++ ];
        if ( this->in_mhdr != NULL ) {
          oh.msg_hdr.msg_name    = this->in_mhdr[ i ].msg_hdr.msg_name;
          oh.msg_hdr.msg_namelen = this->in_mhdr[ i ].msg_hdr.msg_namelen;
        }
        oh.msg_hdr.msg_iov        = &this->strm.iov[ k ];
        oh.msg_hdr.msg_iovlen     = j - k;
        oh.msg_hdr.msg_control    = NULL;
        oh.msg_hdr.msg_controllen = 0;
        oh.msg_hdr.msg_flags      = 0;
        oh.msg_len                = 0;
      }
    }
    return true;
  }
  struct iovec   * iov = (struct iovec *) (void *)
                this->strm.alloc_temp( this->iov_cnt * sizeof( struct iovec ) );
               /** end     = &iov[ this->iov_cnt ];*/
  struct mmsghdr * nh  = NULL;
  MemcachedHdr   * h   = NULL,
                 * h2  = NULL;
  uint32_t         n, cnt, sz;

  if ( iov == NULL )
    return false;
  /* walk through each of the requests and construct outgoing udp frames */
  for ( i = 0, o = 0; i < this->nmsgs; i++ ) {
    k = this->out_idx[ i ];
    j = this->out_idx[ i + 1 ];
    if ( j != k ) {
      struct mmsghdr & oh = this->out_mhdr[ o ];
      n = o++;
      /* the first frame in a request, already has a memcached udp header */
      if ( this->in_mhdr != NULL ) {
        oh.msg_hdr.msg_name    = this->in_mhdr[ i ].msg_hdr.msg_name;
        oh.msg_hdr.msg_namelen = this->in_mhdr[ i ].msg_hdr.msg_namelen;
      }
      oh.msg_hdr.msg_iov        = iov;
      oh.msg_hdr.msg_iovlen     = 0;
      oh.msg_hdr.msg_control    = NULL;
      oh.msg_hdr.msg_controllen = 0;
      oh.msg_hdr.msg_flags      = 0;
      oh.msg_len                = 0;

      cnt  = 0;    /* index into iov[] */
      next = this->frame_size; /* first has hdr, next msgs do not */
      len  = 0;    /* portion of next filled */
      off  = 0;    /* offset into current iov[].iov_base */
      nh   = &oh;  /* the head of the current udp frame */
      do {
        /* if need a new header */
        if ( len == 0 && next == this->frame_size - MC_HDR_SIZE ) {
          nh = &this->out_mhdr[ o++ ];
          h2 = (MemcachedHdr *) (void *)
                                this->strm.alloc_temp( MC_HDR_SIZE );
          if ( this->in_mhdr != NULL ) {
            nh->msg_hdr.msg_name    = this->in_mhdr[ i ].msg_hdr.msg_name;
            nh->msg_hdr.msg_namelen = this->in_mhdr[ i ].msg_hdr.msg_namelen;
          }
          nh->msg_hdr.msg_iov        = &iov[ cnt ];
          nh->msg_hdr.msg_iovlen     = 1;
          nh->msg_hdr.msg_control    = NULL;
          nh->msg_hdr.msg_controllen = 0;
          nh->msg_hdr.msg_flags      = 0;
          nh->msg_len                = 0;
          iov[ cnt ].iov_len  = MC_HDR_SIZE;
          iov[ cnt ].iov_base = h2;
          cnt++;
        }
        /* pack a smaller segment */
        if ( this->strm.iov[ k ].iov_len - off + len < next ) {
          sz = this->strm.iov[ k ].iov_len - off;
          iov[ cnt ].iov_base = &((char *) this->strm.iov[ k ].iov_base)[ off ];
          iov[ cnt ].iov_len  = sz;
          nh->msg_hdr.msg_iovlen++;
          len += sz;
          off = 0;
          cnt++;
          k++;
        }
        /* split a larger segment */
        else { /* iov_len[ k ] + len >= next */
          sz = next - len;
          iov[ cnt ].iov_base = &((char *) this->strm.iov[ k ].iov_base)[ off ];
          iov[ cnt ].iov_len  = sz;
          nh->msg_hdr.msg_iovlen++;
          off += sz;
          cnt++;
          if ( this->strm.iov[ k ].iov_len == off ) {
            k++;
            off = 0;
          }
          len = 0;
          next = this->frame_size - MC_HDR_SIZE;
        }
      } while ( k < j );
      /* fix up the seqno, total for each message in this request */
      if ( cnt > 1 ) {
        h = (MemcachedHdr *) (void *) iov[ 0 ].iov_base;
        h->total = __builtin_bswap16( o - n );
        for ( uint32_t x = 1; n + x < o; x++ ) {
          nh = &this->out_mhdr[ n + x ];
          h2 = (MemcachedHdr *) nh->msg_hdr.msg_iov[ 0 ].iov_base;
          *h2 = *h;
          h2->seqno = __builtin_bswap16( x );
        }
      }
      iov = &iov[ cnt ];
    }
  }
#if 0
  if ( o != this->out_nmsgs || iov != end ) {
    fprintf( stderr, "bad msg count %u %u %u\n", o, this->out_nmsgs,
             this->iov_cnt );
  }
#endif
  return true;
}

void
EvMemcachedService::read( void ) noexcept
{
  size_t nb = this->bytes_recv;
  this->EvConnection::read();
  stat.bytes_read += this->bytes_recv - nb;
}

void
EvMemcachedService::write( void ) noexcept
{
  size_t nb = this->bytes_sent;
  this->EvConnection::write();
  stat.bytes_written += this->bytes_sent - nb;
}

void
EvMemcachedUdp::read( void ) noexcept
{
  size_t nb = this->bytes_recv;
  this->EvUdp::read();
  stat.bytes_read += this->bytes_recv - nb;
}

void
EvMemcachedUdp::write( void ) noexcept
{
  StreamBuf & strm = *this;
  MemcachedUdpFraming g( this->out_idx, this->in_mhdr, strm, this->in_nmsgs );
  if ( strm.sz > 0 )
    strm.flush();
  this->out_idx[ this->in_nmsgs ] = strm.idx; /* extent of iov[] array */
  g.construct_frames();
  this->out_nmsgs = g.out_nmsgs;
  this->out_mhdr  = g.out_mhdr;
  this->out_idx   = NULL;
  size_t nb = this->bytes_sent;
  this->EvUdp::write();
  stat.bytes_written += this->bytes_sent - nb;
}

void
EvMemcachedService::release( void ) noexcept
{
  this->MemcachedExec::release();
  this->EvConnection::release_buffers();
  this->push_free_list();
  stat.curr_connections--;
}

void
EvMemcachedMerge::release( void ) noexcept
{
  if ( this->sav_mhdr != NULL ) {
    for ( uint32_t i = 0; i < this->sav_len; i++ ) {
      if ( this->sav_mhdr[ i ].iov_base != NULL )
        ::free( this->sav_mhdr[ i ].iov_base );
    }
    ::free( this->sav_mhdr );
    this->sav_mhdr = NULL;
    this->sav_len  = 0;
  }
}

void
EvMemcachedUdp::release( void ) noexcept
{
  this->out_idx = NULL;
  this->sav.release();
  this->exec->release();
  this->exec = NULL;
  this->EvUdp::release_buffers();
}

void
EvMemcachedService::push_free_list( void ) noexcept
{
  if ( this->listfl == IN_ACTIVE_LIST )
    fprintf( stderr, "memcached sock should not be in active list\n" );
  else if ( this->listfl != IN_FREE_LIST ) {
    this->listfl = IN_FREE_LIST;
    this->poll.free_memcached.push_hd( this );
  }
}

void
EvMemcachedService::pop_free_list( void ) noexcept
{
  if ( this->listfl == IN_FREE_LIST ) {
    this->listfl = IN_NO_LIST;
    this->poll.free_memcached.pop( this );
  }
}

