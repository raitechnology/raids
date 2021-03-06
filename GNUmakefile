# defines a directory for build, for example, RH6_x86_64
lsb_dist     := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -rs | sed 's/[.].*//' ; else uname -r | sed 's/[-].*//' ; fi)
uname_m      := $(shell uname -m)

short_dist_lc := $(patsubst CentOS,rh,$(patsubst RedHatEnterprise,rh,\
                   $(patsubst RedHat,rh,\
                     $(patsubst Fedora,fc,$(patsubst Ubuntu,ub,\
                       $(patsubst Debian,deb,$(patsubst SUSE,ss,$(lsb_dist))))))))
short_dist    := $(shell echo $(short_dist_lc) | tr a-z A-Z)
pwd           := $(shell pwd)
rpm_os        := $(short_dist_lc)$(lsb_dist_ver).$(uname_m)

# this is where the targets are compiled
build_dir ?= $(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)
bind      := $(build_dir)/bin
libd      := $(build_dir)/lib64
objd      := $(build_dir)/obj
dependd   := $(build_dir)/dep

have_asciidoctor := $(shell if [ -x /usr/bin/asciidoctor ]; then echo true; fi)
have_pandoc := $(shell if [ -x /usr/bin/pandoc ]; then echo true; fi)

# use 'make port_extra=-g' for debug build
ifeq (-g,$(findstring -g,$(port_extra)))
  DEBUG = true
endif

CC          ?= gcc
CXX         ?= g++
cc          := $(CC)
cpp         := $(CXX)
# if not linking libstdc++
ifdef NO_STL
cppflags    := -std=c++11 -fno-rtti -fno-exceptions
cpplink     := $(CC)
else
cppflags    := -std=c++11
cpplink     := $(CXX)
endif
arch_cflags := -mavx -maes -fno-omit-frame-pointer
gcc_wflags  := -Wall -Wextra -Werror
fpicflags   := -fPIC
soflag      := -shared

ifdef DEBUG
default_cflags := -ggdb
else
default_cflags := -ggdb -O3 -Ofast
endif
# rpmbuild uses RPM_OPT_FLAGS
CFLAGS := $(default_cflags)
#RPM_OPT_FLAGS ?= $(default_cflags)
#CFLAGS ?= $(RPM_OPT_FLAGS)
cflags := $(gcc_wflags) $(CFLAGS) $(arch_cflags)

# where to find the raids/xyz.h files
INCLUDES    ?= -Iinclude -Iraikv/include -Iraimd/include
includes    := $(INCLUDES)
DEFINES     ?=
defines     := $(DEFINES)
cpp_lnk     :=
sock_lib    :=
math_lib    := -lm
thread_lib  := -pthread -lrt

# test submodules exist (they don't exist for dist_rpm, dist_dpkg targets)
have_lc_submodule    := $(shell if [ -f ./linecook/GNUmakefile ]; then echo yes; else echo no; fi )
have_md_submodule    := $(shell if [ -f ./raimd/GNUmakefile ]; then echo yes; else echo no; fi )
have_dec_submodule   := $(shell if [ -f ./raimd/libdecnumber/GNUmakefile ]; then echo yes; else echo no; fi )
have_kv_submodule    := $(shell if [ -f ./raikv/GNUmakefile ]; then echo yes; else echo no; fi )
have_h3_submodule    := $(shell if [ -f ./h3/GNUmakefile ]; then echo yes; else echo no; fi )
have_rdb_submodule   := $(shell if [ -f ./rdbparser/GNUmakefile ]; then echo yes; else echo no; fi )

lnk_lib     :=
dlnk_lib    :=
lnk_dep     :=
dlnk_dep    :=

# if building submodules, reference them rather than the libs installed
ifeq (yes,$(have_kv_submodule))
kv_lib      := raikv/$(libd)/libraikv.a
kv_lnk      := raikv/$(libd)/libraikv.a
kv_dll      := raikv/$(libd)/libraikv.so
lnk_lib     += $(kv_lib)
lnk_dep     += $(kv_lib)
dlnk_lib    += -Lraikv/$(libd) -lraikv
dlnk_dep    += $(kv_dll)
rpath1       = ,-rpath,$(pwd)/raikv/$(libd)
else
kv_lnk       = -lraikv
lnk_lib     += -lraikv
dlnk_lib    += -lraikv
endif

ifeq (yes,$(have_lc_submodule))
lc_lib      := linecook/$(libd)/liblinecook.a
lc_dll      := linecook/$(libd)/liblinecook.so
lnk_lib     += $(lc_lib)
lnk_dep     += $(lc_lib)
dlnk_lib    += -Llinecook/$(libd) -llinecook
dlnk_dep    += $(lc_dll)
rpath2       = ,-rpath,$(pwd)/linecook/$(libd)
else
lnk_lib     += -llinecook
dlnk_lib    += -llinecook
endif

ifeq (yes,$(have_md_submodule))
md_lib      := raimd/$(libd)/libraimd.a
md_dll      := raimd/$(libd)/libraimd.so
lnk_lib     += $(md_lib)
lnk_dep     += $(md_lib)
dlnk_lib    += -Lraimd/$(libd) -lraimd
dlnk_dep    += $(md_dll)
rpath3       = ,-rpath,$(pwd)/raimd/$(libd)
else
lnk_lib     += -lraimd
dlnk_lib    += -lraimd
endif

ifeq (yes,$(have_h3_submodule))
h3_lib      := h3/$(libd)/libh3.a
h3_dll      := h3/$(libd)/libh3.so
lnk_lib     += $(h3_lib)
lnk_dep     += $(h3_lib)
dlnk_lib    += -Lh3/$(libd) -lh3
dlnk_dep    += $(h3_dll)
rpath4       = ,-rpath,$(pwd)/h3/$(libd)
else
lnk_lib     += -lh3
dlnk_lib    += -lh3
endif

ifeq (yes,$(have_dec_submodule))
dec_lib     := raimd/libdecnumber/$(libd)/libdecnumber.a
dec_dll     := raimd/libdecnumber/$(libd)/libdecnumber.so
lnk_lib     += $(dec_lib)
lnk_dep     += $(dec_lib)
dlnk_lib    += -Lraimd/libdecnumber/$(libd) -ldecnumber
dlnk_dep    += $(dec_dll)
rpath5       = ,-rpath,$(pwd)/raimd/libdecnumber/$(libd)
else
lnk_lib     += -ldecnumber
dlnk_lib    += -ldecnumber
endif

ifeq (yes,$(have_rdb_submodule))
rdb_lib     := rdbparser/$(libd)/librdbparser.a
rdb_dll     := rdbparser/$(libd)/librdbparser.so
lnk_lib     += $(rdb_lib)
lnk_dep     += $(rdb_lib)
dlnk_lib    += -Lrdbparser/$(libd) -lrdbparser
dlnk_dep    += $(rdb_dll)
rpath6       = ,-rpath,$(pwd)/rdbparser/$(libd)
else
lnk_lib     += -lrdbparser
dlnk_lib    += -lrdbparser
endif

ds_lib      := $(libd)/libraids.a
rpath       := -Wl,-rpath,$(pwd)/$(libd)$(rpath1)$(rpath2)$(rpath3)$(rpath4)$(rpath5)$(rpath6)$(rpath7)
dlnk_lib    += -lpcre2-8 -lcrypto
malloc_lib  :=

.PHONY: everything
everything: $(kv_lib) $(rdb_lib) $(h3_lib) $(dec_lib) $(md_lib) $(lc_lib) all

clean_subs :=
dlnk_dll_depend :=
dlnk_lib_depend :=

# build submodules if have them
ifeq (yes,$(have_kv_submodule))
$(kv_lib) $(kv_dll):
	$(MAKE) -C raikv
.PHONY: clean_kv
clean_kv:
	$(MAKE) -C raikv clean
clean_subs += clean_kv
endif
ifeq (yes,$(have_lc_submodule))
$(lc_lib) $(lc_dll):
	$(MAKE) -C linecook
.PHONY: clean_lc
clean_lc:
	$(MAKE) -C linecook clean
clean_subs += clean_lc
endif
ifeq (yes,$(have_dec_submodule))
$(dec_lib) $(dec_dll):
	$(MAKE) -C raimd/libdecnumber
.PHONY: clean_dec
clean_dec:
	$(MAKE) -C raimd/libdecnumber clean
clean_subs += clean_dec
endif
ifeq (yes,$(have_md_submodule))
$(md_lib) $(md_dll):
	$(MAKE) -C raimd
.PHONY: clean_md
clean_md:
	$(MAKE) -C raimd clean
clean_subs += clean_md
endif
ifeq (yes,$(have_h3_submodule))
$(h3_lib) $(h3_dll):
	$(MAKE) -C h3
.PHONY: clean_h3
clean_h3:
	$(MAKE) -C h3 clean
clean_subs += clean_h3
endif
ifeq (yes,$(have_rdb_submodule))
$(rdb_lib) $(rdb_dll):
	$(MAKE) -C rdbparser
.PHONY: clean_rdb
clean_rdb:
	$(MAKE) -C rdbparser clean
clean_subs += clean_rdb
endif

# copr/fedora build (with version env vars)
# copr uses this to generate a source rpm with the srpm target
-include .copr/Makefile

# debian build (debuild)
# target for building installable deb: dist_dpkg
-include deb/Makefile

# targets filled in below
all_exes    :=
all_libs    :=
all_dlls    :=
all_depends :=
gen_files   :=
server_defines         := -DDS_VER=$(ver_build)
redis_server_defines   := -DDS_VER=$(ver_build) -DGIT_HEAD=$(shell git rev-parse HEAD | cut -c 1-8)
memcached_exec_defines := -DDS_VER=$(ver_build)

redis_geo_includes       := -Ih3/src/h3lib/include -I/usr/include/h3lib
redis_sortedset_includes := -Ih3/src/h3lib/include -I/usr/include/h3lib
decimal_includes         := -Iraimd/libdecnumber/include
ev_client_includes       := -Ilinecook/include
test_stream_includes     := -Ilinecook/include
term_includes            := -Ilinecook/include
redis_rdb_includes       := -Irdbparser/include $(redis_geo_includes)

libraids_files := ev_service ev_http ev_client shm_client redis_msg \
  redis_cmd_db redis_exec redis_keyspace redis_geo redis_hash \
  redis_hyperloglog redis_key redis_list redis_pubsub redis_script redis_set \
  redis_sortedset redis_stream redis_string redis_transaction redis_rdb \
  redis_server redis_api ev_memcached memcached_exec term
libraids_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(libraids_files)))
libraids_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libraids_files)))
libraids_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(libraids_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libraids_files)))
libraids_dlnk  := $(dlnk_lib)
libraids_spec  := $(version)-$(build_num)_$(git_hash)
libraids_ver   := $(major_num).$(minor_num)

$(libd)/libraids.a: $(libraids_objs)
$(libd)/libraids.so: $(libraids_dbjs) $(dlnk_dep)

all_libs    += $(libd)/libraids.a $(libd)/libraids.so
all_depends += $(libraids_deps)

raids_dlib  := $(libd)/libraids.so
raids_dlnk  := -L$(libd) -lraids $(dlnk_lib)

libshmdp_files := shmdp
libshmdp_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libshmdp_files)))
libshmdp_deps  := $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libshmdp_files)))
libshmdp_dlnk  := $(raids_dlnk)
libshmdp_libs  := $(libd)/libraids.so
libshmdp_spec  := $(version)-$(build_num)_$(git_hash)
libshmdp_ver   := $(major_num).$(minor_num)

$(libd)/libshmdp.so: $(libshmdp_dbjs) $(libshmdp_libs)

all_libs    += $(libd)/libshmdp.so
all_depends += $(libshmdp_deps)

ds_server_files := server
ds_server_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ds_server_files)))
ds_server_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ds_server_files)))
ds_server_libs  := $(ds_lib)
ds_server_lnk   := $(ds_lib) $(lnk_lib) -lpcre2-32 -lpcre2-8 -lcrypto -llzf
#ds_server_static_lnk := $(ds_lib) $(lnk_lib) -lpcre2-32 -lpcre2-8 -lcrypto -llzf
#ds_server_lnk        := $(raids_dlnk)

#$(bind)/ds_server: $(ds_server_objs) $(ds_server_libs)
$(bind)/ds_server: $(ds_server_objs) $(ds_lib) $(lnk_dep)

all_exes    += $(bind)/ds_server
all_depends += $(ds_server_deps)

shmdp_files := smain
shmdp_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(shmdp_files)))
shmdp_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(shmdp_files)))
#shmdp_libs  := $(libd)/libshmdp.so
#shmdp_lnk   := -lshmdp $(raids_dlnk)

$(bind)/shmdp: $(shmdp_objs) $(shmdp_libs)

all_exes    += $(bind)/shmdp
all_depends += $(shmdp_deps)

ds_client_files := cli
ds_client_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ds_client_files)))
ds_client_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ds_client_files)))
ds_client_libs  := $(raids_dlib)
ds_client_lnk   := $(raids_dlnk)

$(bind)/ds_client: $(ds_client_objs) $(ds_client_libs)

test_msg_files := test_msg
test_msg_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_msg_files)))
test_msg_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_msg_files)))
test_msg_libs  := $(raids_dlib)
test_msg_lnk   := $(raids_dlnk)

$(bind)/test_msg: $(test_msg_objs) $(test_msg_libs)

test_mcmsg_files := test_mcmsg
test_mcmsg_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_mcmsg_files)))
test_mcmsg_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_mcmsg_files)))
test_mcmsg_libs  := $(raids_dlib)
test_mcmsg_lnk   := $(raids_dlnk)

$(bind)/test_mcmsg: $(test_mcmsg_objs) $(test_mcmsg_libs)

test_cmd_files := test_cmd
test_cmd_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_cmd_files)))
test_cmd_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_cmd_files)))
test_cmd_libs  := $(raids_dlib)
test_cmd_lnk   := $(raids_dlnk)

$(bind)/test_cmd: $(test_cmd_objs) $(test_cmd_libs)

redis_cmd_files := redis_cmd
redis_cmd_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(redis_cmd_files)))
redis_cmd_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(redis_cmd_files)))
redis_cmd_libs  := $(kv_lib)
redis_cmd_lnk   := $(kv_lnk)

$(bind)/redis_cmd: $(redis_cmd_objs) $(redis_cmd_libs)

test_list_files := test_list
test_list_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_list_files)))
test_list_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_list_files)))
test_list_libs  := $(raids_dlib)
test_list_lnk   := $(raids_dlnk)

$(bind)/test_list: $(test_list_objs) $(test_list_libs)

test_hash_files := test_hash
test_hash_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_hash_files)))
test_hash_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_hash_files)))
test_hash_libs  := $(raids_dlib)
test_hash_lnk   := $(raids_dlnk)

$(bind)/test_hash: $(test_hash_objs) $(test_hash_libs)

test_set_files := test_set
test_set_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_set_files)))
test_set_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_set_files)))
test_set_libs  := $(raids_dlib)
test_set_lnk   := $(raids_dlnk)

$(bind)/test_set: $(test_set_objs) $(test_set_libs)

test_zset_files := test_zset
test_zset_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_zset_files)))
test_zset_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_zset_files)))
test_zset_libs  := $(raids_dlib)
test_zset_lnk   := $(raids_dlnk)

$(bind)/test_zset: $(test_zset_objs) $(test_zset_libs)

test_hllnum_files := test_hllnum
test_hllnum_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_hllnum_files)))
test_hllnum_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_hllnum_files)))
test_hllnum_libs  := $(raids_dlib)
test_hllnum_lnk   := $(raids_dlnk)

$(bind)/test_hllnum: $(test_hllnum_objs) $(test_hllnum_libs)

test_hllw_files := test_hllw
test_hllw_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_hllw_files)))
test_hllw_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_hllw_files)))
test_hllw_libs  := $(raids_dlib)
test_hllw_lnk   := $(raids_dlnk)

$(bind)/test_hllw: $(test_hllw_objs) $(test_hllw_libs)

test_hllsub_files := test_hllsub
test_hllsub_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_hllsub_files)))
test_hllsub_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_hllsub_files)))
test_hllsub_libs  := $(raids_dlib)
test_hllsub_lnk   := $(raids_dlnk)

$(bind)/test_hllsub: $(test_hllsub_objs) $(test_hllsub_libs)

test_geo_includes = -Ih3/src/h3lib/include -I/usr/include/h3lib
test_geo_files := test_geo
test_geo_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_geo_files)))
test_geo_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_geo_files)))
test_geo_libs  := $(raids_dlib)
test_geo_lnk   := $(raids_dlnk)

$(bind)/test_geo: $(test_geo_objs) $(test_geo_libs)

test_decimal_files := test_decimal
test_decimal_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_decimal_files)))
test_decimal_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_decimal_files)))
test_decimal_libs  := $(raids_dlib)
test_decimal_lnk   := $(raids_dlnk)

$(bind)/test_decimal: $(test_decimal_objs) $(test_decimal_libs)

test_timer_files := test_timer
test_timer_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_timer_files)))
test_timer_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_timer_files)))
test_timer_libs  := $(raids_dlib)
test_timer_lnk   := $(raids_dlnk)

$(bind)/test_timer: $(test_timer_objs) $(test_timer_libs)

test_ping_files := test_ping
test_ping_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_ping_files)))
test_ping_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_ping_files)))
test_ping_libs  := $(raids_dlib)
test_ping_lnk   := $(raids_dlnk)

$(bind)/test_ping: $(test_ping_objs) $(test_ping_libs)

test_sub_files := test_sub
test_sub_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_sub_files)))
test_sub_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_sub_files)))
test_sub_libs  := $(raids_dlib)
test_sub_lnk   := $(raids_dlnk)

$(bind)/test_sub: $(test_sub_objs) $(test_sub_libs)

test_pub_files := test_pub
test_pub_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_pub_files)))
test_pub_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_pub_files)))
test_pub_libs  := $(raids_dlib)
test_pub_lnk   := $(raids_dlnk)

$(bind)/test_pub: $(test_pub_objs) $(test_pub_libs)

test_stream_files := test_stream
test_stream_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_stream_files)))
test_stream_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_stream_files)))
test_stream_libs  := $(raids_dlib)
test_stream_lnk   := $(raids_dlnk)

$(bind)/test_stream: $(test_stream_objs) $(test_stream_libs)

ds_test_api_files := test_api
ds_test_api_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ds_test_api_files)))
ds_test_api_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ds_test_api_files)))
ds_test_api_libs  := $(ds_lib)
ds_test_api_lnk   := $(ds_lib) $(lnk_lib) -lpcre2-32 -lpcre2-8 -lcrypto -llzf

#$(bind)/test_api: $(test_api_objs) $(test_api_libs)
$(bind)/ds_test_api: $(ds_test_api_objs) $(ds_lib) $(lnk_dep)

ds_pubsub_api_files := pubsub_api
ds_pubsub_api_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(ds_pubsub_api_files)))
ds_pubsub_api_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(ds_pubsub_api_files)))
ds_pubsub_api_libs  := $(ds_lib)
ds_pubsub_api_lnk   := $(ds_lib) $(lnk_lib) -lpcre2-32 -lpcre2-8 -lcrypto -llzf

$(bind)/ds_pubsub_api: $(ds_pubsub_api_objs) $(ds_lib) $(lnk_dep)

all_exes    += $(bind)/ds_client $(bind)/test_msg $(bind)/test_mcmsg \
               $(bind)/redis_cmd $(bind)/test_cmd $(bind)/test_list \
	       $(bind)/test_hash $(bind)/test_set $(bind)/test_zset \
	       $(bind)/test_hllnum $(bind)/test_hllw $(bind)/test_hllsub \
	       $(bind)/test_geo $(bind)/test_decimal $(bind)/test_timer \
	       $(bind)/test_ping $(bind)/test_sub $(bind)/test_pub \
	       $(bind)/test_stream $(bind)/ds_test_api \
               $(bind)/ds_pubsub_api
all_depends += $(ds_client_deps) $(test_msg_deps) $(test_mcmsg_deps) \
               $(redis_cmd_deps) $(test_cmd_deps) $(test_list_deps) \
	       $(test_hash_deps) $(test_set_deps) $(test_zset_deps) \
	       $(test_hllnum_deps) $(test_hllw_deps) $(test_hllsub_deps) \
	       $(test_geo_deps) $(test_decimal_deps) $(test_timer_deps) \
	       $(test_ping_deps) $(test_sub_deps) $(test_pub_deps) \
	       $(test_stream_deps) $(ds_test_api_deps) $(ds_pubsub_api_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

ifeq ($(have_pandoc),true)
doc/ds_server.1: doc/ds_server.1.md
	pandoc -s -t man $< -o $@
gen_files += doc/ds_server.1
endif

ifeq ($(have_asciidoctor),true)
doc/redis_cmd.html: doc/redis_cmd.adoc
	asciidoctor -b html5 doc/redis_cmd.adoc
gen_files += doc/redis_cmd.html
endif
# cmp exchange this in case multiple builds are running
include/raids/redis_cmd.h: $(bind)/redis_cmd doc/redis_cmd.adoc
	$(bind)/redis_cmd doc/redis_cmd.adoc > include/raids/redis_cmd.h.1
	if cmp -s include/raids/redis_cmd.h include/raids/redis_cmd.h.1 ; then \
	  echo no change ; \
	  rm include/raids/redis_cmd.h.1 ; \
	  touch include/raids/redis_cmd.h ; \
	else \
	  mv include/raids/redis_cmd.h.1 include/raids/redis_cmd.h ; \
	fi
gen_files += include/raids/redis_cmd.h

# the default targets
.PHONY: all
all: $(gen_files) $(all_libs) $(all_dlls) $(all_exes)

.PHONY: dnf_depend
dnf_depend:
	sudo dnf -y install make gcc-c++ git redhat-lsb openssl-devel pcre2-devel chrpath liblzf-devel zlib-devel libbsd-devel

.PHONY: yum_depend
yum_depend:
	sudo yum -y install make gcc-c++ git redhat-lsb openssl-devel pcre2-devel chrpath liblzf-devel zlib-devel libbsd-devel

.PHONY: deb_depend
deb_depend:
	sudo apt-get install -y install make g++ gcc devscripts libpcre2-dev chrpath git lsb-release libssl-dev lzf zlib1g-dev uuid-dev libbsd-dev

# create directories
$(dependd):
	@mkdir -p $(all_dirs)

# remove target bins, objs, depends
.PHONY: clean
clean: $(clean_subs)
	rm -r -f $(bind) $(libd) $(objd) $(dependd)
	if [ "$(build_dir)" != "." ] ; then rmdir $(build_dir) ; fi

.PHONY: clean_dist
clean_dist:
	rm -rf dpkgbuild rpmbuild

.PHONY: clean_all
clean_all: clean clean_dist

# force a remake of depend using 'make -B depend'
.PHONY: depend
depend: $(dependd)/depend.make

$(dependd)/depend.make: $(dependd) $(all_depends)
	@echo "# depend file" > $(dependd)/depend.make
	@cat $(all_depends) >> $(dependd)/depend.make

.PHONY: dist_bins
dist_bins: $(all_libs) $(bind)/ds_server $(bind)/shmdp $(bind)/ds_client
	chrpath -d $(libd)/libraids.so
	chrpath -d $(libd)/libshmdp.so
	chrpath -d $(bind)/shmdp
	chrpath -d $(bind)/ds_server
	chrpath -d $(bind)/ds_client

.PHONY: dist_rpm
dist_rpm: srpm
	( cd rpmbuild && rpmbuild --define "-topdir `pwd`" -ba SPECS/raids.spec )

# dependencies made by 'make depend'
-include $(dependd)/depend.make

ifeq ($(DESTDIR),)
# 'sudo make install' puts things in /usr/local/lib, /usr/local/include
install_prefix = /usr/local
else
# debuild uses DESTDIR to put things into debian/raids/usr
install_prefix = $(DESTDIR)/usr
endif

install: dist_bins
	install -d $(install_prefix)/lib $(install_prefix)/bin
	install -d $(install_prefix)/include/raids
	for f in $(libd)/libraids.* $(libd)/libshmdp.* ; do \
	if [ -h $$f ] ; then \
	cp -a $$f $(install_prefix)/lib ; \
	else \
	install $$f $(install_prefix)/lib ; \
	fi ; \
	done
	install -m 755 $(bind)/ds_server $(install_prefix)/bin
	install -m 755 $(bind)/shmdp $(install_prefix)/bin
	install -m 755 $(bind)/ds_client $(install_prefix)/bin
	install -m 644 include/raids/*.h $(install_prefix)/include/raids

$(objd)/%.o: src/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: src/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.cpp
	$(cpp) $(cflags) $(fpicflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.fpic.o: src/%.c
	$(cc) $(cflags) $(fpicflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.cpp
	$(cpp) $(cflags) $(cppflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(objd)/%.o: test/%.c
	$(cc) $(cflags) $(includes) $(defines) $($(notdir $*)_includes) $($(notdir $*)_defines) -c $< -o $@

$(libd)/%.a:
	ar rc $@ $($(*)_objs)

$(libd)/%.so:
	$(cpplink) $(soflag) $(rpath) $(cflags) -o $@.$($(*)_spec) -Wl,-soname=$(@F).$($(*)_ver) $($(*)_dbjs) $($(*)_dlnk) $(cpp_dll_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib) && \
	cd $(libd) && ln -f -s $(@F).$($(*)_spec) $(@F).$($(*)_ver) && ln -f -s $(@F).$($(*)_ver) $(@F)

$(bind)/%:
	$(cpplink) $(cflags) $(rpath) -o $@ $($(*)_objs) -L$(libd) $($(*)_lnk) $(cpp_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib)

$(bind)/%.static:
	$(cpplink) $(cflags) -o $@ $($(*)_objs) $($(*)_static_lnk) $(sock_lib) $(math_lib) $(thread_lib) $(malloc_lib) $(dynlink_lib)

$(dependd)/%.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.fpic.d: src/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.fpic.d: src/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.d: test/%.cpp
	$(cpp) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.c
	$(cc) $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

