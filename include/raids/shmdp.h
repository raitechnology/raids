#ifndef __raids__shmdp_h__
#define __raids__shmdp_h__

#include <raids/ev_client.h>

namespace rai {
namespace ds {

void shmdp_initialize( const char *mn,  int pt ) noexcept;
void shmdp_atexit( void ) noexcept;

static const size_t FD_MAP_SZ = 8 * 1024;
typedef uint64_t fd_map_t[ FD_MAP_SZ / sizeof( uint64_t ) ];

struct FdMap {
  fd_map_t map;
  int      max_fd;

  bool fd_add( int fd ) {
    if ( fd < 0 || (size_t) fd >= FD_MAP_SZ )
      return false;
    size_t off  = fd / 64,
           shft = fd % 64;
    this->map[ off ] |= (uint64_t) 1 << shft;
    if ( fd >= this->max_fd )
      this->max_fd = fd + 1;
    return true;
  }

  bool fd_test( int fd ) {
    if ( fd < 0 || fd >= this->max_fd )
      return false;
    size_t off  = fd / 64,
           shft = fd % 64;
    return ( this->map[ off ] & ( (uint64_t) 1 << shft ) ) != 0;
  }

  void fd_clr( int fd ) {
    if ( fd < 0 || (size_t) fd >= FD_MAP_SZ )
      return;
    size_t off  = fd / 64,
           shft = fd % 64;
    this->map[ off ] &= ~( (uint64_t) 1 << shft );
    if ( fd + 1 == this->max_fd ) {
      while ( fd > 0 ) {
        if ( this->fd_test( --fd ) ) {
          this->max_fd = fd + 1;
          return;
        }
      }
      this->max_fd = 0;
    }
  }

  bool fd_first( int &fd ) {
    fd = -1;
    return this->fd_next( fd );
  }

  bool fd_next( int &fd ) {
    if ( ++fd >= this->max_fd )
      return false;
    size_t off  = fd / 64,
           shft = fd % 64;
    for (;;) {
      if ( this->map[ off ] != 0 ) {
        for ( ; shft < 64; shft++ ) {
          if ( ( this->map[ off ] & ( (uint64_t) 1 << shft ) ) != 0 ) {
            fd = off * 64 + shft;
            return fd < this->max_fd;
          }
        }
      }
      if ( ++off * 64 >= (size_t) this->max_fd )
        return false;
      shft = 0;
    }
  }
};

struct QueueFd;
struct QueuePoll : public EvCallback {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  EvPoll      poll;
  EvShmClient shm;
  QueueFd  ** fds;
  FdMap       pending;
  int         fds_size;
  bool        idle,
              inprogress;

  QueuePoll() : shm( this->poll, *this ), fds( 0 ),
                fds_size( 0 ), idle( true ), inprogress( false ) {
    ::memset( &this->pending, 0, sizeof( this->pending ) );
  }
  void unlink( QueueFd *p ) noexcept;
  bool push( QueueFd *p ) noexcept;
  ssize_t read( int fd, char *buf, size_t count ) noexcept;
  ssize_t write( int fd, const char *buf, size_t count ) noexcept;
  QueueFd *find( int fd, bool create ) noexcept;
};

struct QueueFd {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  QueuePoll & queue;
  int         fd;
  char      * in_buf;
  size_t      in_off,
              in_len,
              in_buflen;
  char      * out_buf;
  size_t      out_off,
              out_len,
              out_buflen;

  QueueFd( int fildes,  QueuePoll &q )
    : queue( q ), fd( fildes ),
      in_buf( 0 ), in_off( 0 ), in_len( 0 ), in_buflen( 0 ),
      out_buf( 0 ), out_off( 0 ), out_len( 0 ), out_buflen( 0 ) {}
  ~QueueFd() {
    if ( this->in_buf != NULL )
      ::free( this->in_buf );
    if ( this->out_buf != NULL )
      ::free( this->out_buf );
  }
};

}
}
#endif
