#define _GNU_SOURCE 1
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <dlfcn.h>
#include <pwd.h>
#include <errno.h>
#include <raids/redis_exec.h>
#include <raids/shmdp.h>

using namespace rai;
using namespace ds;

FdMap       conn,
            pollin,
            pollout;
QueuePoll * qp;

#define DBG( x ) ;

static int ds_pair[ 2 ];
static in_addr_t ds_local;
static in_port_t ds_port;
static uint64_t ds_connect,
                ds_connect_real,
                ds_epoll_ctl,
                ds_epoll_ctl_real,
                ds_epoll_wait,
                ds_epoll_wait_real,
                ds_read,
                ds_read_real,
                ds_write,
                ds_write_real,
                ds_msg_count;

static void
init_port( int pt )
{
  if ( pt == 0 ) {
    const char *p = ::getenv( "RAIDS_PORT" );
    if ( p == NULL || (pt = (in_port_t) atoi( p )) == 0 ) {
      fprintf( stderr, "RAIDS_PORT env var not set\n" );
      exit( 11 );
    }
  }
  ds_local = htonl( 0x7f000001U );
  ds_port  = htons( (uint16_t) pt );
  ::socketpair( PF_LOCAL, SOCK_STREAM, 0, ds_pair );
}

void
rai::ds::shmdp_atexit( void ) noexcept
{
  if ( qp != NULL ) {
    qp->poll.quit++;
    for (;;) {
      if ( qp->poll.quit >= 5 )
        break;
      qp->poll.wait( qp->idle == EvPoll::DISPATCH_IDLE ? 100 : 0 );
      qp->idle = qp->poll.dispatch();
    }
  }
  printf( "\r\n%lu connect\r\n"
          "%lu connect_real\r\n"
          "%lu epoll_ctl\n"
          "%lu epoll_ctl_real\r\n"
          "%lu epoll_wait\r\n"
          "%lu epoll_wait_real\r\n"
          "%lu read\r\n"
          "%lu read_real\r\n"
          "%lu write\r\n"
          "%lu write_real\r\n"
          "%lu msg_count\r\n",
   ds_connect,
   ds_connect_real,
   ds_epoll_ctl,
   ds_epoll_ctl_real,
   ds_epoll_wait,
   ds_epoll_wait_real,
   ds_read,
   ds_read_real,
   ds_write,
   ds_write_real,
   ds_msg_count );

  printf( "bye\n" );
}

void
rai::ds::shmdp_initialize( const char *mn,  int pt ) noexcept
{
  void * m = aligned_malloc( sizeof( QueuePoll ) );
  if ( m == NULL ) {
    perror( "malloc" );
    exit( 9 );
  }
  qp = new ( m ) QueuePoll();
  qp->poll.init( 16, false/*, false*/ );
  if ( mn == NULL ) {
    if ( (mn = ::getenv( "RAIDS_SHM" )) == NULL ) {
      fprintf( stderr, "RAIDS_SHM env var not set\n" );
      exit( 10 );
    }
  }
  if ( ds_port == 0 || pt != 0 )
    init_port( pt );
  if ( qp->shm.open( mn, 0 /* db */ ) == 0 &&
       qp->poll.init_shm( qp->shm ) == 0 ) {
    if ( qp->shm.init_exec() == 0 ) {
      atexit( shmdp_atexit );
      return;
    }
  }
  fprintf( stderr, "Failed to open SHM %s\n", mn );
  exit( 12 );
}

extern "C" {

int
connect( int sockfd, const struct sockaddr *addr, socklen_t addrlen )
{
  static int ( *connect_next )( int, const struct sockaddr *, socklen_t );

  if ( ds_port == 0 )
    init_port( 0 );
  if ( connect_next == NULL )
    connect_next = (int (*)(int, const struct sockaddr *, socklen_t))
                   dlsym( RTLD_NEXT, "connect" );
  if ( addr->sa_family == AF_INET ) {
    const struct sockaddr_in *addr_in = (const struct sockaddr_in *) addr;
    if ( addr_in->sin_addr.s_addr == ds_local &&
         addr_in->sin_port == ds_port ) {
      if ( qp == NULL )
        shmdp_initialize( NULL, 0 );
      DBG( fprintf( stderr, "add %d\n", sockfd ) );
      int flags = fcntl( sockfd, F_GETFL );
      dup2( ds_pair[ 0 ], sockfd );
      fcntl( sockfd, F_SETFL, flags );
      conn.fd_add( sockfd );
      ds_connect++;
      return 0;
    }
  }
  ds_connect_real++;
  return connect_next( sockfd, addr, addrlen );
}

int
setsockopt( int sockfd, int level, int optname, const void *optval,
            socklen_t len )
{
  static int ( *setsockopt_next )( int, int, int, const void *, socklen_t );

  if ( setsockopt_next == NULL )
    setsockopt_next = (int (*)(int, int, int, const void *, socklen_t))
                   dlsym( RTLD_NEXT, "setsockopt" );
  if ( conn.fd_test( sockfd ) )
    return 0;
  return setsockopt_next( sockfd, level, optname, optval, len );
}

static void
set_opt( void *opt, socklen_t *len, int val )
{
  for (;;) {
    switch ( *len ) {
      case 8: *(int64_t *) opt = val; return;
      case 4: *(int32_t *) opt = val; return;
      case 2: *(int16_t *) opt = val; return;
      case 1: *(int8_t *) opt = val;  return;
      default:
        if ( *len >= 4 )      { *len = 4; break; }
        else if ( *len >= 1 ) { *len = 1; break; }
        return;
    }
  }
}

int
getsockopt( int sockfd, int level, int optname, void *optval,
            socklen_t *len )
{
  static int ( *getsockopt_next )( int, int, int, void *, socklen_t * );

  if ( getsockopt_next == NULL )
    getsockopt_next = (int (*)(int, int, int, void *, socklen_t *))
                   dlsym( RTLD_NEXT, "getsockopt" );
  if ( conn.fd_test( sockfd ) ) {
    if ( level == SOL_SOCKET && optname == SO_ERROR )
      set_opt( optval, len, 0 );
    return 0;
  }
  return getsockopt_next( sockfd, level, optname, optval, len );
}

int
getsockname( int sockfd, struct sockaddr *addr, socklen_t *addrlen )
{
  static int ( *getsockname_next )( int, struct sockaddr *, socklen_t * );

  if ( getsockname_next == NULL )
    getsockname_next = (int (*)(int, struct sockaddr *, socklen_t *))
                   dlsym( RTLD_NEXT, "getsockname" );
  if ( conn.fd_test( sockfd ) ) {
    struct sockaddr_in *addr_in = (struct sockaddr_in *) addr;
    if ( *addrlen >= sizeof( struct sockaddr_in ) ) {
      addr_in->sin_family = AF_INET;
      addr_in->sin_addr.s_addr = ds_local;
      addr_in->sin_port = htons( 10000 + sockfd );
      *addrlen = sizeof( struct sockaddr_in );
      return 0;
    }
  }
  return getsockname_next( sockfd, addr, addrlen );
}

int
getpeername( int sockfd, struct sockaddr *addr, socklen_t *addrlen )
{
  static int ( *getpeername_next )( int, struct sockaddr *addr, socklen_t * );

  if ( getpeername_next == NULL )
    getpeername_next = (int (*)(int, struct sockaddr *, socklen_t *))
                   dlsym( RTLD_NEXT, "getpeername" );
  if ( conn.fd_test( sockfd ) ) {
    struct sockaddr_in *addr_in = (struct sockaddr_in *) addr;
    if ( *addrlen >= sizeof( struct sockaddr_in ) ) {
      addr_in->sin_family = AF_INET;
      addr_in->sin_addr.s_addr = ds_local;
      addr_in->sin_port = ds_port;
      *addrlen = sizeof( struct sockaddr_in );
      return 0;
    }
  }
  return getpeername_next( sockfd, addr, addrlen );
}

int
close( int fd )
{
  static int ( *close_next )( int );
  int status;

  if ( close_next == NULL )
    close_next = (int (*)(int)) dlsym( RTLD_NEXT, "close" );
  if ( (status = close_next( fd )) == 0 )
    conn.fd_clr( fd );
  return status;
}

int
epoll_ctl( int epfd, int op, int fd, struct epoll_event *event )
{
  static int ( *epoll_ctl_next )( int, int, int, struct epoll_event * );

  if ( epoll_ctl_next == NULL )
    epoll_ctl_next = (int (*)(int, int, int, struct epoll_event *))
                     dlsym( RTLD_NEXT, "epoll_ctl" );
  if ( conn.fd_test( fd ) ) {
    ds_epoll_ctl++;
    switch ( op ) {
      case EPOLL_CTL_MOD:
      case EPOLL_CTL_ADD:
        if ( ( event->events & EPOLLIN ) != 0 ) {
          DBG( fprintf( stderr, "pollin %d\n", fd ) );
          pollin.fd_add( fd );
        }
        else
          pollin.fd_clr( fd );
        if ( ( event->events & EPOLLOUT ) != 0 ) {
          DBG( fprintf( stderr, "pollout %d\n", fd ) );
          pollout.fd_add( fd );
        }
        else
          pollout.fd_clr( fd );
        break;
      case EPOLL_CTL_DEL:
        pollin.fd_clr( fd );
        pollout.fd_clr( fd );
        break;
    }
    return 0;
  }
  ds_epoll_ctl_real++;
  return epoll_ctl_next( epfd, op, fd, event );
}

int
select( int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds,
        struct timeval *timeout )
{
  static int ( *select_next )( int, fd_set *, fd_set *, fd_set *,
                               struct timeval * );
  int fd, cnt = 0;
  size_t i;
  if ( select_next == NULL )
    select_next = (int (*)(int, fd_set *, fd_set *, fd_set *, struct timeval *))
                  dlsym( RTLD_NEXT, "select" );
  if ( qp == NULL || qp->inprogress )
    return select_next( nfds, readfds, writefds, exceptfds, timeout );
  if ( conn.fd_first( fd ) ) {
    int rmap[ FD_MAP_SZ ], wmap[ FD_MAP_SZ ], emap[ FD_MAP_SZ ];
    size_t rcnt = 0, wcnt = 0, ecnt = 0;
    do {
      if ( fd >= nfds )
        break;
      if ( readfds != NULL && FD_ISSET( fd, readfds ) != 0 &&
           qp->pending.fd_test( fd ) )
        rmap[ rcnt++ ] = fd;
      if ( writefds != NULL && FD_ISSET( fd, writefds ) != 0 )
        wmap[ wcnt++ ] = fd;
      if ( exceptfds != NULL && FD_ISSET( fd, exceptfds ) != 0 )
        emap[ ecnt++ ] = fd;
    } while ( conn.fd_next( fd ) );

    if ( ( rcnt | wcnt ) != 0 ) {
      static const size_t NFB = sizeof( readfds->fds_bits[ 0 ] ) * 8;
      for ( i = 0; (int) ( i * NFB ) < nfds; i++ ) {
        if ( readfds != NULL )
          readfds->fds_bits[ i ] = 0;
        if ( writefds != NULL )
          writefds->fds_bits[ i ] = 0;
        if ( exceptfds != NULL )
          exceptfds->fds_bits[ i ] = 0;
      }
      for ( i = 0; i < rcnt; i++ ) {
        FD_SET( rmap[ i ], readfds );
        cnt++;
      }
      for ( i = 0; i < wcnt; i++ ) {
        FD_SET( wmap[ i ], writefds );
        cnt++;
      }
      return cnt;
    }
    else {
      for ( i = 0; i < rcnt; i++ )
        FD_CLR( rmap[ i ], readfds );
      for ( i = 0; i < wcnt; i++ )
        FD_CLR( wmap[ i ], writefds );
      for ( i = 0; i < ecnt; i++ )
        FD_CLR( emap[ i ], exceptfds );
    }
  }
  qp->inprogress = true;
  if ( qp->idle == EvPoll::DISPATCH_IDLE )
    qp->poll.wait( 0 );
  qp->idle = qp->poll.dispatch();
  qp->inprogress = false;
  if ( qp->idle != EvPoll::DISPATCH_IDLE )
    return 0;
  return select_next( nfds, readfds, writefds, exceptfds, timeout );
}

int
epoll_wait( int epfd, struct epoll_event *events,
            int maxevents, int timeout )
{
  static int ( *epoll_wait_next )( int, struct epoll_event *, int, int );
  if ( epoll_wait_next == NULL )
    epoll_wait_next = (int (*)(int, struct epoll_event *, int, int))
      dlsym( RTLD_NEXT, "epoll_wait" );
  if ( qp == NULL || qp->inprogress )
    return epoll_wait_next( epfd, events, maxevents, timeout );
  ds_epoll_wait++;
  int i = 0;
  int fdbase = 0;
  if ( maxevents > 0 && qp != NULL ) {
    for ( size_t off = 0; ; ) {
      uint64_t fdin  = pollin.map[ off ] & qp->pending.map[ off ],
               fdout = pollout.map[ off ];
      if ( ( fdin | fdout ) != 0 ) {
        for ( int shft = 0; shft < 64; shft++ ) {
          uint64_t mask = (uint64_t) 1 << shft;
          if ( ( ( fdin | fdout ) & mask ) != 0 ) {
            events[ i ].data.fd = fdbase + shft;
            events[ i ].events  = 0;
            if ( ( fdin & mask ) != 0 )
              events[ i ].events |= EPOLLIN;
            if ( ( fdout & mask ) != 0 )
              events[ i ].events |= EPOLLOUT;
            if ( ++i == maxevents )
              return i;
          }
        }
      }
      off++;
      fdbase += 64;
      if ( fdbase >= pollout.max_fd &&
           ( fdbase >= pollin.max_fd || fdbase >= qp->pending.max_fd ) )
        break;
    }
    if ( i > 0 ) {
      DBG( fprintf( stderr, "epoll_wait %d\n", i ) );
      return i;
    }
  }
  qp->inprogress = true;
  if ( qp->idle == EvPoll::DISPATCH_IDLE )
    qp->poll.wait( 0 );
  qp->idle = qp->poll.dispatch();
  qp->inprogress = false;
  if ( qp->idle != EvPoll::DISPATCH_IDLE )
    return 0;
  ds_epoll_wait_real++;
  return epoll_wait_next( epfd, events, maxevents, timeout );
}

ssize_t
read( int fd, void *buf, size_t count )
{
  static ssize_t ( *read_next )( int, void *, size_t count );
  ssize_t status;

  if ( read_next == NULL )
    read_next = (ssize_t (*)(int, void *, size_t)) dlsym( RTLD_NEXT, "read" );
  if ( qp != NULL && conn.fd_test( fd ) ) {
    ds_read++;
    status = qp->read( fd, (char *) buf, count );
    if ( status == 0 ) {
      errno = EAGAIN;
      status = -1;
    }
  }
  else {
    ds_read_real++;
    status = read_next( fd, buf, count );
  }
  return status;
}

ssize_t
recv( int sockfd, void *buf, size_t len, int flags )
{
  static ssize_t ( *recv_next )(int, void *, size_t, int);
  ssize_t status;

  if ( recv_next == NULL )
    recv_next = (ssize_t (*)(int, void *, size_t, int))
      dlsym( RTLD_NEXT, "recv" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    ds_read++;
    status = qp->read( sockfd, (char *) buf, len );
    if ( status == 0 ) {
      errno = EAGAIN;
      status = -1;
    }
  }
  else {
    ds_read_real++;
    status = recv_next( sockfd, buf, len, flags );
  }
  return status;
}

ssize_t
recvfrom( int sockfd, void *buf, size_t len, int flags,
          struct sockaddr *src_addr, socklen_t *addrlen )
{
  static ssize_t ( *recvfrom_next )(int, void *, size_t, int, struct sockaddr *,
                                    socklen_t *);
  ssize_t status;

  if ( recvfrom_next == NULL )
    recvfrom_next = (ssize_t (*)(int, void *, size_t, int flags,
                                 struct sockaddr *, socklen_t *))
      dlsym( RTLD_NEXT, "recvfrom" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    ds_read++;
    status = qp->read( sockfd, (char *) buf, len );
    if ( status == 0 ) {
      errno = EAGAIN;
      status = -1;
    }
    if ( addrlen != NULL )
      *addrlen = 0;
  }
  else {
    ds_read_real++;
    status = recvfrom_next( sockfd, buf, len, flags, src_addr, addrlen );
  }
  return status;
}

ssize_t
recvmsg( int sockfd, struct msghdr *msg, int flags )
{
  static ssize_t ( *recvmsg_next )(int, struct msghdr *, int);
  ssize_t status;

  if ( recvmsg_next == NULL )
    recvmsg_next = (ssize_t (*)(int, struct msghdr *, int))
      dlsym( RTLD_NEXT, "recvmsg" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    size_t i, nbytes = 0;
    ds_read++;
    for ( i = 0; i < msg->msg_iovlen; i++ ) {
      void * buf = msg->msg_iov[ i ].iov_base;
      size_t len = msg->msg_iov[ i ].iov_len;
      status = qp->read( sockfd, (char *) buf, len );
      msg->msg_iov[ i ].iov_len = status;
      nbytes += status;
    }
    if ( (status = nbytes) == 0 ) {
      errno = EAGAIN;
      status = -1;
    }
    msg->msg_namelen = 0;
    msg->msg_controllen = 0;
    msg->msg_flags = 0;
  }
  else {
    ds_read_real++;
    status = recvmsg_next( sockfd, msg, flags );
  }
  return status;
}

ssize_t
write( int fd, const void *buf, size_t count )
{
  static ssize_t ( *write_next )( int, const void *, size_t count );
  ssize_t status;

  if ( write_next == NULL )
    write_next = (ssize_t (*)(int, const void *, size_t))
      dlsym( RTLD_NEXT, "write" );
  if ( qp != NULL && conn.fd_test( fd ) ) {
    ds_write++;
    status = qp->write( fd, (const char *) buf, count );
  }
  else {
    ds_write_real++;
    status = write_next( fd, buf, count );
  }
  return status;
}

ssize_t
send( int sockfd, const void *buf, size_t len, int flags )
{
  static ssize_t (* send_next )( int, const void *, size_t, int );
  ssize_t status;
  if ( send_next == NULL )
    send_next = (ssize_t (*)(int, const void *, size_t, int))
      dlsym( RTLD_NEXT, "send" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    ds_write++;
    status = qp->write( sockfd, (const char *) buf, len );
  }
  else {
    ds_write_real++;
    status = send_next( sockfd, buf, len, flags );
  }
  return status;
}

ssize_t
sendto( int sockfd, const void *buf, size_t len, int flags,
        const struct sockaddr *dest_addr, socklen_t addrlen )
{
  static ssize_t (* sendto_next )( int, const void *, size_t, int,
                                   const struct sockaddr *, socklen_t );
  ssize_t status;
  if ( sendto_next == NULL )
    sendto_next = (ssize_t (*)(int, const void *, size_t, int,
                               const struct sockaddr *, socklen_t))
      dlsym( RTLD_NEXT, "sendto" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    ds_write++;
    status = qp->write( sockfd, (const char *) buf, len );
  }
  else {
    ds_write_real++;
    status = sendto_next( sockfd, buf, len, flags, dest_addr, addrlen );
  }
  return status;
}

ssize_t
sendmsg( int sockfd, const struct msghdr *msg, int flags )
{
  static ssize_t (* sendmsg_next )( int, const struct msghdr *, int );
  ssize_t status;
  if ( sendmsg_next == NULL )
    sendmsg_next = (ssize_t (*)(int, const struct msghdr *, int))
      dlsym( RTLD_NEXT, "sendmsg" );
  if ( qp != NULL && conn.fd_test( sockfd ) ) {
    size_t i, nbytes = 0;
    ds_write++;
    for ( i = 0; i < msg->msg_iovlen; i++ ) {
      const void * buf = msg->msg_iov[ i ].iov_base;
      size_t       len = msg->msg_iov[ i ].iov_len;
      status = qp->write( sockfd, (char *) buf, len );
      if ( status < 0 )
        return status;
      nbytes += status;
    }
    status = nbytes;
  }
  else {
    ds_write_real++;
    status = sendmsg_next( sockfd, msg, flags );
  }
  return status;
}


} /* extern "C" */


ssize_t
QueuePoll::read( int fd, char *buf, size_t count ) noexcept
{
  QueueFd      * p   = this->find( fd, false );
  size_t         off = 0,
                 size,
                 buflen,
                 i, j;
  RedisMsgStatus mstatus;
  ExecStatus     status;

  DBG( fprintf( stderr, "read %d %d\n", fd, p == NULL ) );
  if ( p == NULL )
    return 0;
  if ( p->out_off < p->out_len ) {
    size = kv::min<size_t>( count, p->out_len - p->out_off );
    ::memcpy( &buf[ off ], &p->out_buf[ p->out_off ], size );
    off += size;
    p->out_off += size;
  }
  for (;;) {
    if ( off == count ) /* no more space */
      break;
    buflen = p->in_len - p->in_off;
    if ( buflen == 0 ) /* no more msgs */
      break;
    mstatus = this->shm.exec->msg.unpack( &p->in_buf[ p->in_off ], buflen,
                                          this->shm.tmp );
    if ( mstatus != DS_MSG_STATUS_OK ) {
      if ( mstatus != DS_MSG_STATUS_PARTIAL ) {
        fprintf( stderr, "protocol error(%d/%s), ignoring %lu bytes\n",
                 mstatus, ds_msg_status_string( mstatus ), buflen );
        p->in_off = p->in_len;
        break;
      }
    }
    ds_msg_count++;
    p->in_off += buflen;
    if ( (status = this->shm.exec->exec( NULL, NULL )) == EXEC_OK )
      if ( this->shm.alloc_fail )
        status = ERR_ALLOC_FAIL;
    switch ( status ) {
      case EXEC_SETUP_OK:
        this->shm.exec->exec_run_to_completion();
        if ( ! this->shm.alloc_fail )
          break;
        status = ERR_ALLOC_FAIL;
        /* FALLTHRU */
      default:
        this->shm.exec->send_status( status, KEY_OK );
        break;
      case EXEC_QUIT:
      case EXEC_DEBUG:
        break;
    }
    if ( this->shm.sz > 0 )
      this->shm.flush();
    for ( i = 0; i < this->shm.idx; i++ ) {
      size_t len  = this->shm.iov[ i ].iov_len;
      void * base = this->shm.iov[ i ].iov_base;
      size = kv::min<size_t>( count - off, len );
      ::memcpy( &buf[ off ], base, size );
      off += size;
      /* save the rest, no space in read buf */
      if ( len > size ) {
        size_t needed = len - size;
        for ( j = i + 1; j < this->shm.idx; j++ )
          needed += this->shm.iov[ j ].iov_len;
        if ( p->out_buflen < needed ) {
          void * m = ::realloc( p->out_buf, needed );
          if ( m == NULL ) {
            errno = ENOSPC;
            return -1;
          }
          p->out_buf = (char *) m;
          p->out_buflen = needed;
        }
        p->out_off = 0;
        p->out_len = len - size;
        ::memcpy( p->out_buf, &((char *) base)[ size ], p->out_len );
        for ( j = i + 1; j < this->shm.idx; j++ ) {
          len  = this->shm.iov[ j ].iov_len;
          base = this->shm.iov[ j ].iov_base;
          ::memcpy( &p->out_buf[ p->out_len ], base, len );
          p->out_len += len;
        }
        break;
      }
    }
    this->shm.reset();
  }
  if ( p->in_off == p->in_len && p->out_off == p->out_len )
    this->pending.fd_clr( fd );
  return off;
}

ssize_t
QueuePoll::write( int fd, const char *buf, size_t count ) noexcept
{
  QueueFd * p = this->find( fd, true );

  DBG( fprintf( stderr, "write %d %d %d\n", fd, p != NULL, p == this->find( fd, false ) ) );
  if ( p == NULL ) {
    errno = ENOSPC;
    return -1;
  }
  if ( p->in_off > 0 ) {
    p->in_len -= p->in_off;
    if ( p->in_len > 0 )
      ::memmove( p->in_buf, &p->in_buf[ p->in_off ], p->in_len );
    p->in_off = 0;
  }
  if ( count + p->in_len > p->in_buflen ) {
    void *m = ::realloc( p->in_buf, count + p->in_len );
    if ( m == NULL ) {
      errno = ENOSPC;
      return -1;
    }
    p->in_buf    = (char *) m;
    p->in_buflen = count + p->in_len;
  }
  ::memcpy( &p->in_buf[ p->in_len ], buf, count );
  p->in_len += count;
  return count;
}

void
QueuePoll::unlink( QueueFd *p ) noexcept
{
  this->fds[ p->fd ] = NULL;
  this->pending.fd_clr( p->fd );
  delete p;
}

bool
QueuePoll::push( QueueFd *p ) noexcept
{
  if ( ! this->pending.fd_add( p->fd ) )
    return false;
  if ( p->fd >= this->fds_size ) {
    int end = this->fds_size;
    void * m;
    if ( (m = ::realloc( this->fds,
                         ( p->fd + 1 ) * sizeof( this->fds[ 0 ] ) )) == NULL ) {
      this->pending.fd_clr( p->fd );
      return false;
    }
    this->fds = (QueueFd **) m;
    this->fds_size = p->fd + 1;
    while ( end < p->fd )
      this->fds[ end++ ] = NULL;
  }
  this->fds[ p->fd ] = p;
  return true;
}

QueueFd *
QueuePoll::find( int fd,  bool is_write ) noexcept
{
  QueueFd *p = NULL;
  if ( fd >= this->fds_size || this->fds[ fd ] == NULL ) {
    if ( is_write ) {
      void *m = aligned_malloc( sizeof( QueueFd ) );
      if ( m == NULL )
        return NULL;
      p = new ( m ) QueueFd( fd, *this );
      if ( ! this->push( p ) ) {
        delete p;
        return NULL;
      }
    }
  }
  else {
    p = this->fds[ fd ];
    if ( is_write )
      this->pending.fd_add( fd );
  }
  return p;
}
