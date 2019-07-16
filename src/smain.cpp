#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

static const char *
get_arg( int argc, char *argv[], int b, const char *f, const char *def )
{
  for ( int i = 1; i < argc - b; i++ )
    if ( ::strcmp( f, argv[ i ] ) == 0 ) /* -m map -p port */
      return argv[ i + b ];
  return def; /* default value */
}

int
main( int argc, char *argv[] )
{
  static const int PATHSZ = 1024;
  char          path[ PATHSZ ];
  const char  * libshmdp = "libshmdp.so";
  int           i;

  const char * pt = get_arg( argc, argv, 1, "-p", "8888" ),
             * ma = get_arg( argc, argv, 1, "-m", "sysv2m:shm.test" ),
             * cm = get_arg( argc, argv, 1, "-c", NULL ),
             * he = get_arg( argc, argv, 0, "-h", 0 );
  if ( he != NULL || cm == NULL ) {
    printf( "%s [-p port] [-m map] -c prog ...\n", argv[ 0 ] );
    return 0;
  }

  const char *slash = ::strrchr( argv[ 0 ], '/' );
  if ( slash != NULL ) {
    const char *s = argv[ 0 ];
    for ( i = 0; i < PATHSZ; i++ ) {
      path[ i ] = *s;
      if ( s++ == slash ) {
        s = "../lib64/libshmdp.so";
        for ( i++; i < PATHSZ; i++ ) {
          path[ i ] = *s;
          if ( *s++ == '\0' )
            break;
        }
        break;
      }
    }
    if ( i == PATHSZ ) {
      fprintf( stderr, "path too large\n" );
      return 100;
    }
    libshmdp = path;
  }
  if ( ::access( libshmdp, X_OK ) != 0 ) {
    ::perror( libshmdp );
    return 101;
  }
  for ( i = 1; i < argc; i++ )
    if ( argv[ i ] == cm )
      break;
  ::setenv( "RAIDS_SHM",  ma, 1 );
  ::setenv( "RAIDS_PORT", pt, 1 );
  ::setenv( "LD_PRELOAD", libshmdp, 1 );
  ::execvp( argv[ i ], &argv[ i ] );

  return 102;
}

