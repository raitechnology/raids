#ifndef __rai_raids__redis_api_h__
#define __rai_raids__redis_api_h__

#include <raids/redis_msg.h>
#include <raids/redis_cmd.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ds_s ds_t;

struct ds_event_s {
  ds_msg_t subject,      /* subject */
           subscription, /* pat matched, subject subscribed, or ibx */
           reply;        /* reply if rpc */
};
typedef struct ds_event_s ds_event_t;
typedef struct kv_geom_s kv_geom_t; /* include <raikv/shm_ht.h> */

typedef void (*ds_on_msg_t)( const ds_event_t *event,
                             const ds_msg_t *msg,
                             void *cl );

/* open ds shared memory: db_num 0 -> 253 (254,255 resvd) */
int ds_open( ds_t **h, const char *map_name,  uint8_t db_num,
             int use_busy_poll );
/* create ds shared memory: db_num 0 -> 253 (254,255 resvd) */
int ds_create( ds_t **h, const char *map_name,  uint8_t db_num,
               int use_busy_poll,  kv_geom_t *geom,  int map_mode );
/* unique context id assigned for this thread on open/create, 0 -> 127 */
int ds_get_ctx_id( ds_t *h );
/* close ds shared memory */
int ds_close( ds_t *h );
/* process epoll events */
int ds_dispatch( ds_t *h,  int ms );
/* fetch next result */
int ds_result( ds_t *h,  ds_msg_t *result );
/* parse cmd string, run and fill result, return 0 if success */
int ds_run( ds_t *h,  ds_msg_t *result,  const char *str );
/* execute cmd and fill result, return 0 if success */
int ds_run_cmd( ds_t *h,  ds_msg_t *result,  ds_msg_t *cmd );
/* parse string, which can be RESP format or just command string */
int ds_parse_msg( ds_t *h,  ds_msg_t *result,  const char *str );
/* convert msg into json string */
int ds_msg_to_json( ds_t *h,  const ds_msg_t *msg,  ds_msg_t *json );
/* allocate space in temporary mem, released by ds_release_mem() */
void *ds_alloc_mem( ds_t *h,  size_t sz );
/* release temprary mem used by all of the above functions, after ds_close() */
int ds_release_mem( ds_t *h );
/* subscribe using callback */
int ds_subscribe_with_cb( ds_t *h,  const ds_msg_t *subject,
                          ds_on_msg_t cb,  void *cl );
/* subscribe using callback */
int ds_psubscribe_with_cb( ds_t *h,  const ds_msg_t *subject,
                           ds_on_msg_t cb,  void *cl );

#ifdef __cplusplus
}
#endif
#endif
