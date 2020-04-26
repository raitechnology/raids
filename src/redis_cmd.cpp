#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <raids/redis_cmd_db.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <ctype.h>

using namespace rai;
using namespace ds;

static size_t
copy_str( const char *s,  const char *e,  char *buf,  size_t buflen )
{
  size_t len = e - s;
  if ( len > buflen - 1 )
    len = buflen - 1;
  ::memcpy( buf, s, len );
  buf[ len ] = '\0';
  return len;
}

struct GenCmdMan {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }

  GenCmdMan    * next;
  char         * text;
  size_t         textlen;
  RedisExtraType mantype;

  GenCmdMan( const char *txt,  size_t len,  RedisExtraType type ) {
    this->next = NULL;
    this->text = (char *) &this[ 1 ];
    ::memcpy( this->text, txt, len );
    this->text[ len ] = '\0';
    this->textlen = len;
    this->mantype = type;
  }
  const char *xtra_type( void ) {
    switch ( this->mantype ) {
      case XTRA_SHORT:   return "XTRA_SHORT";
      case XTRA_USAGE:   return "XTRA_USAGE";
      case XTRA_EXAMPLE: return "XTRA_EXAMPLE";
      case XTRA_DESCR:   return "XTRA_DESCR";
      case XTRA_RETURN:  return "XTRA_RETURN";
    }
    return "XTRA_BAD";
  }
  void put_string( void ) {
    fputs( "\"", stdout );
    for ( size_t i = 0; i < this->textlen; i++ ) {
      switch ( this->text[ i ] ) {
        case '\\':
          fputs( "\\\\", stdout );
          break;
        case '\"':
          fputs( "\\\"", stdout );
          break;
        case '\n':
          if ( i == this->textlen - 1 )
            fputs( "\\n", stdout );
          else
            fputs( "\\n\"\n\"", stdout );
          break;
        default:
          fputc( this->text[ i ], stdout );
          break;
      }
    }
    fputs( "\"", stdout );
  }
  void put_comment( void ) {
    fputs( "/* ", stdout );
    for ( size_t i = 0; i < this->textlen; i++ ) {
      switch ( this->text[ i ] ) {
        case '\n':
          if ( i != this->textlen - 1 )
            fputs( "\n", stdout );
          break;
        default:
          fputc( this->text[ i ], stdout );
          break;
      }
    }
    fputs( " */\n", stdout );
  }
};

struct GenCmdDB {
  char        catg[ MAX_CATG_LEN ];
  char        cmd[ MAX_CMD_LEN ];
  uint8_t     catglen, cmdlen;
  int16_t     tab[ 4 ];
  uint32_t    flags;
  GenCmdMan * mhd,
            * mtl;
  size_t      mcount;

  /* initialize this with cmd name and category, arity tab, flags */
  void cpy_cmd( const char *cg,  size_t cglen,
                const char *c,  size_t clen,  int16_t *t,  uint32_t fl ) {
    this->catglen = copy_str( cg, &cg[ cglen ],
                              this->catg, sizeof( this->catg ) );
    this->cmdlen  = copy_str( c, &c[ clen ], this->cmd, sizeof( this->cmd ) );
    ::memcpy( this->tab, t, sizeof( this->tab ) );
    this->flags = fl;
  }
  /* add an extra section */
  void add_man( const char *text,  size_t len,  RedisExtraType type ) {
    GenCmdMan * m = new ( ::malloc( sizeof( GenCmdMan ) + len + 1 ) )
      GenCmdMan( text, len, type );
    if ( this->mtl == NULL )
      this->mhd = m;
    else
      this->mtl->next = m;
    this->mtl = m;
    this->mcount++;
  }
  const char *flags_enum( void ) {
    switch ( this->flags ) {
      default:                return "CMD_NOFLAGS";
      case CMD_ADMIN_FLAG:    return "CMD_ADMIN_FLAG";
      case CMD_READ_FLAG:     return "CMD_READ_FLAG";
      case CMD_WRITE_FLAG:    return "CMD_WRITE_FLAG";
      case CMD_MOVABLE_FLAG:  return "CMD_MOVABLE_FLAG";
      case CMD_READ_MV_FLAG:  return "CMD_READ_MV_FLAG";
      case CMD_WRITE_MV_FLAG: return "CMD_WRITE_MV_FLAG";
    }
  }
  /* append _CMD to command string for enum */
  const char *cmd_enum( void ) {
    static char command[ MAX_CMD_LEN + 8 ];
    for ( size_t i = 0; i < this->cmdlen; i++ ) {
      command[ i ] = toupper( this->cmd[ i ] );
      if ( command[ i ] == ' ' )
        command[ i ] = '_';
    }
    command[ this->cmdlen ] = '_';
    ::strcpy( &command[ this->cmdlen + 1 ], "CMD" );
    return command;
  }
  /* append _CATG to category string for enum */
  const char *catg_enum( void ) {
    static char category[ MAX_CATG_LEN + 8 ];
    for ( size_t i = 0; i < this->catglen; i++ ) {
      category[ i ] = toupper( this->catg[ i ] );
      if ( category[ i ] == ' ' )
        category[ i ] = '_';
    }
    category[ this->catglen ] = '_';
    ::strcpy( &category[ this->catglen + 1 ], "CATG" );
    return category;
  }
};

static GenCmdDB gen_db[ 256 ];
static size_t   gen_db_len;

static GenCmdDB *
new_command( void )
{
  if ( gen_db_len == 256 ) {
    fprintf( stderr, "Too many commands (256)\n" );
    exit( 1 );
  }
  return &gen_db[ gen_db_len++ ];
}

static GenCmdDB *
find_command( const char *cmd,  size_t cmdlen )
{
  for ( size_t i = 0; i < gen_db_len; i++ ) {
    if ( gen_db[ i ].cmdlen == cmdlen &&
         ::memcmp( gen_db[ i ].cmd, cmd, cmdlen ) == 0 )
      return &gen_db[ i ];
  }
  return NULL;
}

static void
gen_db_enums( void )
{
  const size_t maxlen = 18;
  char prev_cat[ MAX_CATG_LEN ];
  const char *cat;
  size_t i, len;
  uint32_t cnt;

  /* STRING_CATG enums */
  prev_cat[ 0 ] = '\0';
  printf( "enum ds_category {\n  NO_CATG%*s =  0", (int) ( maxlen - 3 ), "" );
  for ( i = 0, cnt = 1; i < gen_db_len; i++ ) {
    cat = gen_db[ i ].catg;
    if ( ::strcmp( cat, prev_cat ) != 0 ) {
      ::strcpy( prev_cat, cat );
      len = ::strlen( cat );
      printf( ",\n  %s%*s = %2u", gen_db[ i ].catg_enum(),
              (int) ( maxlen - len - 1 ), "", cnt++ );
    }
  }
  printf( "\n};\n\n" );

  /* GET_CMD enums */
  prev_cat[ 0 ] = '\0';
  printf( "enum ds_command {\n  NO_CMD%*s  =   0", (int) ( maxlen - 3 ), "" );
  for ( i = 0, cnt = 1; i < gen_db_len; i++ ) {
    cat = gen_db[ i ].catg;
    if ( ::strcmp( cat, prev_cat ) != 0 ) {
      ::strcpy( prev_cat, cat );
      printf( ",\n  /* %s */\n", prev_cat );
    }
    else {
      printf( ",\n" );
    }
    len = gen_db[ i ].cmdlen;
    printf( "  %s%*s = %3u",
            gen_db[ i ].cmd_enum(), (int) ( maxlen - len ), "", cnt++ );
  }
  printf( "\n};\n\n" );
}

static uint32_t
command_hash( const void *cmd,  size_t len,  uint32_t seed )
{
  uint32_t out[ MAX_CMD_LEN / 4 ];
 for ( size_t i = 0; i * 4 < len; i++ )
   out[ i ] = ((const uint32_t *) cmd)[ i ] & 0xdfdfdfdfU;
  return kv_crc_c( out, len, seed );
}

static uint32_t
gen_db_hash( void )
{
  static const uint32_t HTSZ = 1024;
  kv::rand::xoroshiro128plus rand;
  uint64_t     x = 0;
  uint32_t     ht[ HTSZ ];
  size_t       i;
  const char * cmd;
  size_t       len;
  uint32_t     r = /*0x8eafcc7e*/ 0x36fbdccd;
  int          k = 0;

  /* find a seed that has no collisions within HTSZ elems */
  rand.init();
  goto try_first;
  for (;;) {
    if ( ( k & 1 ) == 0 ) {
      x = rand.next();
      r = (uint32_t) x;
    }
    else {
      r = x >> 32;
    }
  try_first:;
    ::memset( ht, 0, sizeof( ht ) );
    for ( i = 0; i < gen_db_len; i++ ) {
      cmd = gen_db[ i ].cmd;
      len = gen_db[ i ].cmdlen;

      uint32_t h = command_hash( cmd, len, r );
      uint32_t *p = &ht[ h % HTSZ ];
      if ( h == 0 || *p != 0 )
        goto try_next;
      *p = h;
    }
    break;
  try_next:;
    k++;
  }

  /* the hash table: ht[ crc % HTSZ ] -> cmd enum */
  printf( "static const uint8_t cmd_hash_ht[ %u ] = {\n", HTSZ );
  ::memset( ht, 0, sizeof( ht ) );
  for ( i = 0; i < gen_db_len; i++ ) {
    cmd = gen_db[ i ].cmd;
    len = gen_db[ i ].cmdlen;

    uint32_t h = command_hash( cmd, len, r );
    ht[ h % HTSZ ] = i + 1;
  }
  printf( "%u", ht[ 0 ] );
  for ( i = 1; i < HTSZ; i++ ) {
    if ( ( i % 32 ) == 0 )
      printf( ",\n%u", ht[ i ] );
    else
      printf( ",%u", ht[ i ] );
  }
  printf( "};\n\n" );

  /* convenience functions for command name to enum */
  printf( "static inline uint32_t\n"
          "get_redis_cmd_hash( const void *cmd,  size_t len ) {\n"
          "  uint32_t out[ MAX_CMD_LEN / 4 ];\n"
          "  for ( size_t i = 0; i * 4 < len; i++ )\n"
          "    out[ i ] = ((const uint32_t *) cmd)[ i ] & 0xdfdfdfdfU;\n"
          "  return kv_crc_c( out, len, 0x%x );\n"
          "}\n\n", r );
  printf( "static inline RedisCmd\n"
          "get_redis_cmd( uint32_t h ) {\n"
         "  return (RedisCmd) cmd_hash_ht[ h %% %u ];\n"
         "}\n\n", HTSZ );
  return r;
}

static int
gen_cmd_db( void )
{
  size_t i, j, cnt;

  /* the generates the redis_cmd.h file */
  printf( "#ifndef __rai_raids__redis_cmd_h__\n"
          "#define __rai_raids__redis_cmd_h__\n"
          "\n"
          "/* file generated by redis_cmd < redis_cmd.adoc > redis_cmd.h */\n"
          "\n"
          "#ifdef __cplusplus\n"
          "extern \"C\" {\n"
          "#endif\n"
          "\n" );

  /* the STRING_CATG enum and the GET_CMD enum */
  gen_db_enums();

  printf( "typedef struct ds_s ds_t;\n"
          "typedef struct ds_msg_s ds_msg_t;\n"
          "\n" );

  static const int MAX_USED = 32;
  static const int MAX_PAT  = 16;
  int8_t arity, first, last, step;
  int    arg, end, num, knum;
  char   pat[ MAX_PAT ], used[ MAX_USED ][ MAX_PAT ];
  int    poff, u, uoff = 0;

  ::memset( used, 0, sizeof( used ) );
  for ( i = 0; i < gen_db_len; i++ ) {
    GenCmdDB & db = gen_db[ i ];
    arity = db.tab[ 0 ];
    first = db.tab[ 1 ];
    last  = db.tab[ 2 ];
    step  = db.tab[ 3 ];

    end = ( arity >= 0 ) ? arity : -arity;
    knum = num = poff = 0;
    if ( last < 0 )
      last = end + last;
    for ( arg = 1; arg < end; arg++ ) {
      if ( first == 0 || arg < first || arg > last ||
           ( arg - first ) % step != 0 )
        pat[ poff++ ] = 'A';
      else
        pat[ poff++ ] = 'K';
    }
    if ( arity < 0 )
      pat[ poff++ ] = 'O';
    assert( poff < MAX_PAT );
    pat[ poff ] = '\0';
    int n = 1;
    for ( u = 0; u < uoff; u++ ) {
      n = ::strcmp( used[ u ], pat );
      if ( n >= 0 )
        break;
    }
    if ( n != 0 ) {
      assert( uoff < MAX_USED );
      ::memmove( &used[ u + 1 ], &used[ u ],
                 &used[ uoff ][ 0 ] - &used[ u ][ 0 ] );
      ::memset( used[ u ], 0, sizeof( used[ u ] ) );
      ::strcpy( used[ u ], pat );
      uoff++;
    }
  }
  printf( "#ifndef REDIS_XTRA\n"
  "/* The function signatures used below: K = key, A = arg, O = optional */\n"
  );
  for ( u = 0; u < uoff; u++ ) {
    printf( "#define _F_%s( cmd ) ;\n", used[ u ] );
  }
  printf( "#endif\n"
"/* _F_CRC checks that the generated funs are defined in redis_cmd_db.cpp */\n"
"/* this forces all of the following decls to be available */\n"
          "#define _F_CRC 0x%08xU\n"
          "#define _F_CNT %d\n"
          "\n", kv_crc_c( used, sizeof( used ), 0 ), uoff );
  printf(
"/* The ds_ functions are defined in the library (redis_cmd_db.cpp) to keep\n"
"   them independent of CMD enum changes above;  the library interface will\n"
"   stay the same after cmds are added, but the enums may be reordered */\n\n");
  for ( i = 0; i < gen_db_len; i++ ) {
    GenCmdDB & db = gen_db[ i ];
    arity = db.tab[ 0 ];
    first = db.tab[ 1 ];
    last  = db.tab[ 2 ];
    step  = db.tab[ 3 ];
    GenCmdMan * m = db.mhd;

    for ( ; m != NULL; m = m->next ) {
      if ( m->mantype == XTRA_USAGE )
        m->put_comment();
    }
    printf( "int ds_%s( ds_t *h, ds_msg_t *r", db.cmd );
    end = ( arity >= 0 ) ? arity : -arity;
    knum = num = poff = 0;
    if ( last < 0 )
      last = end + last;
    for ( arg = 1; arg < end; arg++ ) {
      if ( first == 0 || arg < first || arg > last ||
           ( arg - first ) % step != 0 ) {
        printf( ", ds_msg_t *arg" );
        if ( num++ != 0 )
          printf( "%d", num );
        pat[ poff++ ] = 'A';
      }
      else {
        printf( ", ds_msg_t *key" );
        if ( knum++ != 0 )
          printf( "%d", knum );
        pat[ poff++ ] = 'K';
      }
    }
    if ( arity < 0 ) {
      printf( ", ds_msg_t *opt, ... )" );
      pat[ poff++ ] = 'O';
    }
    else
      printf( " )" );
    printf( " _F_%.*s( %s )\n", poff, pat, db.cmd_enum() );
  }

  printf( "\n"
          "#ifdef __cplusplus\n"
          "}\n"
          "\n"
          "#include <raids/redis_cmd_db.h>\n"
          "#include <raikv/key_hash.h>\n"
          "\n"
          "namespace rai {\n"
          "namespace ds {\n"
          "\n"
          "typedef enum ds_category RedisCatg;\n"
          "typedef enum ds_command  RedisCmd;\n"
          "\n" );

  /* gen hash table and functions, returns hash seed */
  uint32_t r = gen_db_hash();

  /* extra info */
  cnt = 0;
  for ( i = 0; i < gen_db_len; i++ )
    cnt += gen_db[ i ].mcount;
  printf( "extern const RedisCmdExtra xtra[ %lu ];\n\n", cnt );

  /* generate table of commands */
  cnt = 0;
  printf( "static const size_t REDIS_CMD_DB_SIZE = %lu;\n"
          "static const RedisCmdData\n"
          "cmd_db[ REDIS_CMD_DB_SIZE ] = {\n"
          "{ \"none\",NULL,0,NO_CMD,CMD_NOFLAGS,4,NO_CATG,0,0,0,0 }",
          gen_db_len + 1 );
  for ( i = 0; i < gen_db_len; i++ ) {
    GenCmdDB & db = gen_db[ i ];
    uint32_t h = command_hash( db.cmd, db.cmdlen, r );
    if ( db.mcount == 0 ) {
      printf( "empty extra! %s\n", db.cmd );
      return 1;
    }
               /*  name    extra,   hash,cmd,flags,cmdlen, catg, tab[] */
    printf( ",\n{ \"%s\",&xtra[%lu],0x%x,%s,%s,%u,%s,%d,%d,%d,%d }",
          db.cmd, cnt, h, db.cmd_enum(), db.flags_enum(), db.cmdlen,
          db.catg_enum(), db.tab[ 0 ], db.tab[ 1 ], db.tab[ 2 ], db.tab[ 3 ] );
    cnt += db.mcount;
  }
  printf( "\n};\n"
          "\n"
          "#ifdef REDIS_XTRA\n" );

  /* generate extra list */
  printf( "const RedisCmdExtra xtra[ %lu ] = {\n", cnt );

  cnt = 0;
  for ( i = 0; i < gen_db_len; i++ ) {
    GenCmdDB  & db = gen_db[ i ];
    GenCmdMan * p  = db.mhd;
    for ( j = 0; j < db.mcount; j++ ) {
      if ( j + 1 < db.mcount )
        printf( "{ &xtra[%lu], ", cnt + 1 );
      else
        printf( "{ NULL, " );
      printf( "%s,%s", p->xtra_type(), ( p->textlen > 45 ) ? "\n" : " " );
      p->put_string();
      printf( "},\n" );
      cnt++;
      p = p->next;
    }
  }
  printf( "};\n"
          "#endif\n"
          "}\n"
          "}\n"
          "#endif\n"
          "#endif\n" );
  return 0;
}

static int
parse_cmd_db( const char *fn )
{
  struct stat  sb;
  const void * addr;
  int          fd = ::open( fn, O_RDONLY );
  
  /* mmap the file */
  if ( fd < 0 || ::fstat( fd, &sb ) < 0 ) {
    ::perror( fn );
    if ( fd >= 0 )
      ::close( fd );
    return 1;
  }
  else {
    addr = ::mmap( NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0 );
    if ( addr == MAP_FAILED ) {
      ::perror( fn );
      ::close( fd );
      return 1;
    }
  }
  const char d[] = "----------------------------";
  char         catg[ MAX_CATG_LEN ],
               cmd[ MAX_CMD_LEN ];
  const void * p;
  const char * eol           = (const char *) addr,
             * bol           = eol,                /* advances by line */
             * end           = &bol[ sb.st_size ], /* end of file */
             * eos           = NULL;
  size_t       llen          = 0, /* line len */
               cmdlen        = 0, /* cmd[] size */
               catglen       = 0, /* catg[] size */
               tabcnt        = 0; /* the arity tab counter */
  bool         in_preformat  = false, /* whether inside ^----\n fmt */
               in_comment    = false, /* whether inside ^////\n */
               new_section   = false; /* section of command info */
  int          matched_catg  = 0, /* matched: Hash\n----\n */
               matched_cmd   = 0; /* matched: [[cmd]]\n */
  const char * curr_p        = NULL, /* current section pointer */
             * next_p        = NULL; /* next section pointer */
  RedisExtraType curr_t      = XTRA_SHORT, /* what section type it is */
                 next_t      = XTRA_SHORT; /* the next section type */
  uint32_t     flags         = 0; /* bits of CMD_READ_FLAG, CMD_WRITE_FLAG */
  int16_t      tab[ 4 ];
  GenCmdDB   * dbp = NULL;

  catg[ 0 ] = '0';
  for (;; bol = &eol[ 1 ] ) {
    /* find the next line */
    if ( (p = ::memchr( bol, '\n', end - bol )) == NULL )
      break;
    new_section = false;
    eol  = (const char *) p;
    llen = eol - bol;
    /* take out comments, which are 4 slashes at bol */
    if ( llen == 4 && ::memcmp( bol, "////", 4 ) == 0 ) {
      in_comment = ! in_comment;
      continue;
    }
    else if ( in_comment )
      continue;

    /* look for section headers of the form:
     * Hash
     * ---- */
    if ( ! in_preformat ) {
      if ( llen > 2 &&
           llen < sizeof( d ) &&
           &eol[ llen + 2 ] < end &&
           eol[ 1 + llen ] == '\n' &&
           ::memcmp( &eol[ 1 ], d, llen ) == 0 ) {
        catglen = copy_str( bol, eol, catg, sizeof( catg ) );
        matched_catg = 2;
        next_p = NULL;
        new_section = true;
      }
    }
    /* match "----" in usage and example sections:
     * .Usage
     * ----
     * scan curs [match pat] [count int]
     * ---- */
    if ( matched_catg == 0 ) {
      if ( llen == 4 && ::memcmp( bol, d, 4 ) == 0 ) {
        in_preformat = ! in_preformat; /* track whether inside or outside */
      }
    }
    if ( in_preformat )
      continue;
    /* match the command table:
     * |================
     * | <<cmd>> | ... */
    if ( ::memcmp( bol, "| <<", 4 ) == 0 ) {
      cmd[ 0 ] = '\0';
      for ( eos = &bol[ 4 ]; eos < eol; eos++ ) {
        if ( eos[ 0 ] == '>' ) {
          cmdlen = copy_str( &bol[ 4 ], eos, cmd, sizeof( cmd ) );
          break;
        }
      }
      /* if found a command, parse the command arity table */
      if ( cmd[ 0 ] != '\0' ) {
        flags  = 0;
        tabcnt = 0;
        /* match | int */
        for ( ; eos < eol; eos++ ) {
          if ( eos[ 0 ] == '|' && eos[ 1 ] == ' ' &&
               ( eos[ 2 ] == '-' || ( eos[ 2 ] >= '0' && eos[ 2 ] <= '9' ) ) ) {
            if ( tabcnt < 4 )
              tab[ tabcnt++ ] = atoi( &eos[ 2 ] );
          }
          /* maybe make this more dynamic in the future */
          else if ( ::memcmp( eos, "read", 4 ) == 0 )
            flags |= CMD_READ_FLAG;
          else if ( ::memcmp( eos, "write", 5 ) == 0 )
            flags |= CMD_WRITE_FLAG;
          else if ( ::memcmp( eos, "admin", 5 ) == 0 )
            flags |= CMD_ADMIN_FLAG;
          else if ( ::memcmp( eos, "movable", 5 ) == 0 )
            flags |= CMD_MOVABLE_FLAG;
        }
        /* 4 == arity, first key, last key, step */
        if ( tabcnt == 4 ) {
          new_command()->cpy_cmd( catg, catglen, cmd, cmdlen, tab, flags );
          /*printf( "%s.%s [%d,%d,%d,%d,%x]\n", catg, cmd,
                  tab[ 0 ], tab[ 1 ], tab[ 2 ], tab[ 3 ], flags );*/
        }
      }
    }
    /* if [[cmd]] */
    else if ( ::memcmp( bol, "[[", 2 ) == 0 &&
              ::memcmp( &eol[ -2 ], "]]\n", 3 ) == 0 ) {
      cmdlen = copy_str( &bol[ 2 ], &eol[ -2 ], cmd, sizeof( cmd ) );
      matched_cmd = 2;
      next_p = NULL;
      new_section = true;
    }
    /* after [[cmd]], parse the short description in the title */
    else if ( matched_cmd == 1 ) {
      for ( eos = bol; eos < eol; eos++ ) {
        if ( eos[ 0 ] == ' ' && eos[ 1 ] == '|' && eos[ 2 ] == ' ' ) {
          char   dshort[ 80 ];
          size_t len = copy_str( &eos[ 3 ], eol, dshort, sizeof( dshort ) );
          dbp = find_command( cmd, cmdlen );
          if ( dbp == NULL ) {
            fprintf( stderr, "cmd %s not found\n", cmd );
            exit( 1 );
          }
          dbp->add_man( dshort, len, XTRA_SHORT );
          /*printf( "%s.%s: %s\n", catg, cmd, dshort );*/
          break;
        }
      }
    }
    /* parse sections: usage, example, description, return */
    if ( ! new_section ) {
      const char * e = &eol[ 1 ];
      next_p = NULL;
      if ( e == end )
        new_section = true;
      else {
        switch ( e[ 0 ] ) {
          case '.':
            if ( &e[ 7 ] < end && ::memcmp( ".Usage", e, 6 ) == 0 ) {
              next_p = &e[ 7 ];
              next_t = XTRA_USAGE;
              new_section = true;
            }
            else if ( &e[ 9 ] < end && ::memcmp( ".Example", e, 8 ) == 0 ) {
              next_p = &e[ 9 ];
              next_t = XTRA_EXAMPLE;
              new_section = true;
            }
            else if ( &e[ 13 ] < end &&
                      ::memcmp( ".Description", e, 12 ) == 0 ) {
              next_p = &e[ 13 ];
              next_t = XTRA_DESCR;
              new_section = true;
            }
            else if ( &e[ 8 ] < end && ::memcmp( ".Return", e, 7 ) == 0 ) {
              next_p = &e[ 8 ];
              next_t = XTRA_RETURN;
              new_section = true;
            }
            break;
          case '/':
            if ( &e[ 5 ] < end && ::memcmp( e, "////", 4 ) == 0 )
              new_section = true;
            break;
          case '[':
            if ( &e[ 3 ] < end && ::memcmp( e, "[[", 2 ) == 0 )
              new_section = true;
            break;
        }
      }
    }
    if ( new_section ) {
      if ( curr_p != NULL ) {
        size_t len = &eol[ 1 ] - curr_p;
        if ( dbp != NULL ) {
          if ( matched_catg != 0 )
            len -= catglen + 1;
          if ( len > 10 ) {
            if ( ::memcmp( curr_p, d, 4 ) == 0 &&
                 ::memcmp( &curr_p[ len - 5 ], d, 4 ) == 0 ) {
              len -= 10;
              curr_p = &curr_p[ 5 ];
            }
            while ( len > 1 && curr_p[ len - 1 ] == '\n' &&
                    curr_p[ len - 2 ] == '\n' )
              len -= 1;
          }
          dbp->add_man( curr_p, len, curr_t );
        }
      }
      curr_p = next_p;
      curr_t = next_t;
      new_section = false;
      next_p = NULL;
    }
    /* these are forward matched */
    if ( matched_catg > 0 )
      matched_catg -= 1;
    if ( matched_cmd > 0 )
      matched_cmd -= 1;
  }
  ::munmap( (void *) addr, sb.st_size );
  ::close( fd );
  return 0;
}

int
main( int argc, char *argv[] )
{
  if ( argc == 1 ) {
    fprintf( stderr, "usage: %s <cmd.adoc>\n", argv[ 0 ] );
    return 1;
  }
  if ( parse_cmd_db( argv[ 1 ] ) != 0 )
    return 1;
  return gen_cmd_db();
}
