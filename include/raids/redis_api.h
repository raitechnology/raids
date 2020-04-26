#ifndef __rai_raids__redis_api_h__
#define __rai_raids__redis_api_h__

#include <raids/redis_msg.h>
#include <raids/redis_cmd.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ds_s ds_t;

/* open ds shared memory: db_num 0 -> 253 (254,255 resvd) */
int ds_open( ds_t **h, const char *map_name,  uint8_t db_num );
/* create ds shared memory: db_num 0 -> 253 (254,255 resvd) */
int ds_create( ds_t **h, const char *map_name,  uint8_t db_num,
             uint64_t map_size,  double entry_ratio,  uint64_t max_value_size );
/* close ds shared memory */
int ds_close( ds_t *h );
/* process epoll events */
int ds_dispatch( ds_t *h,  int ms,  ds_msg_t *result );
/* parse cmd string, run and fill result, return 0 if success */
int ds_run( ds_t *h,  ds_msg_t *result,  const char *str );
/* execute cmd and fill result, return 0 if success */
int ds_run_cmd( ds_t *h,  ds_msg_t *result,  ds_msg_t *cmd );
/* parse string, which can be RESP format or just command string */
int ds_parse_msg( ds_t *h,  ds_msg_t *result,  const char *str );
/* convert msg into json string */
int ds_msg_to_json( ds_t *h,  ds_msg_t *msg,  ds_msg_t *json );
/* allocate space in temporary mem, released by ds_release_mem() */
void *ds_alloc_mem( ds_t *h,  size_t sz );
/* release temprary mem used by all of the above functions, after ds_close() */
int ds_release_mem( ds_t *h );

#ifdef __cplusplus
}
#endif
#endif
