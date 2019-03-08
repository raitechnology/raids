# defines a directory for build, for example, RH6_x86_64
lsb_dist     := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -is ; else echo Linux ; fi)
lsb_dist_ver := $(shell if [ -x /usr/bin/lsb_release ] ; then lsb_release -rs | sed 's/[.].*//' ; fi)
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

# use 'make port_extra=-g' for debug build
ifeq (-g,$(findstring -g,$(port_extra)))
  DEBUG = true
endif

CC          ?= gcc
CXX         ?= g++
cc          := $(CC)
cpp         := $(CXX)
cppflags    := -fno-rtti -fno-exceptions
arch_cflags := -march=corei7-avx -fno-omit-frame-pointer
cpplink     := $(CC)
gcc_wflags  := -Wall -Wextra -Werror
fpicflags   := -fPIC
soflag      := -shared

ifdef DEBUG
default_cflags := -ggdb
else
default_cflags := -ggdb -O3
endif
# rpmbuild uses RPM_OPT_FLAGS
CFLAGS := $(default_cflags)
#RPM_OPT_FLAGS ?= $(default_cflags)
#CFLAGS ?= $(RPM_OPT_FLAGS)
cflags := $(gcc_wflags) $(CFLAGS) $(arch_cflags)

# where to find the raids/xyz.h files
INCLUDES    ?= -Iinclude -Iraikv/include
includes    := $(INCLUDES)
DEFINES     ?=
defines     := $(DEFINES)
cpp_lnk     :=
sock_lib    :=
math_lib    := -lm
thread_lib  := -pthread -lrt

# test submodules exist (they don't exist for dist_rpm, dist_dpkg targets)
have_lc_submodule  := $(shell if [ -d ./linecook ]; then echo yes; else echo no; fi )
have_dec_submodule := $(shell if [ -d ./libdecnumber ]; then echo yes; else echo no; fi )
have_kv_submodule  := $(shell if [ -d ./raikv ]; then echo yes; else echo no; fi )
have_h3_submodule  := $(shell if [ -d ./h3 ]; then echo yes; else echo no; fi )

lnk_lib     :=
dlnk_lib    :=

# if building submodules, reference them rather than the libs installed
ifeq (yes,$(have_kv_submodule))
kv_lib      := raikv/$(libd)/libraikv.a
lnk_lib     += $(kv_lib)
dlnk_lib    += -Lraikv/$(libd) -lraikv
rpath1       = ,-rpath,$(pwd)/raikv/$(libd)
else
lnk_lib     += -lraikv
dlnk_lib    += -lraikv
endif

ifeq (yes,$(have_lc_submodule))
lc_lib      := linecook/$(libd)/liblinecook.a
lnk_lib     += $(lc_lib)
dlnk_lib    += -Llinecook/$(libd) -llinecook
rpath2       = ,-rpath,$(pwd)/linecook/$(libd)
else
lnk_lib     += -llinecook
dlnk_lib    += -llinecook
endif

ifeq (yes,$(have_dec_submodule))
dec_lib     := libdecnumber/$(libd)/libdecnumber.a
lnk_lib     += $(dec_lib)
dlnk_lib    += -Llibdecnumber/$(libd) -ldecnumber
rpath3       = ,-rpath,$(pwd)/libdecnumber/$(libd)
else
lnk_lib     += -ldecnumber
dlnk_lib    += -ldecnumber
endif

ifeq (yes,$(have_h3_submodule))
h3_lib      := h3/$(libd)/libh3.a
lnk_lib     += $(h3_lib)
dlnk_lib    += -Lh3/$(libd) -lh3
rpath4       = ,-rpath,$(pwd)/h3/$(libd)
else
lnk_lib     += -lh3
dlnk_lib    += -lh3
endif

ds_lib      := $(libd)/libraids.a
rpath       := -Wl,-rpath,$(pwd)/$(libd)$(rpath1)$(rpath2)$(rpath3)$(rpath4)
dlnk_lib    += -lpcre2-8 -lcrypto
lnk_lib     += -lpcre2-8 -lcrypto
malloc_lib  :=

.PHONY: everything
everything: $(kv_lib) $(h3_lib) $(dec_lib) $(lc_lib) all

# build submodules if have them
ifeq (yes,$(have_kv_submodule))
$(kv_lib):
	$(MAKE) -C raikv
endif
ifeq (yes,$(have_lc_submodule))
$(lc_lib):
	$(MAKE) -C linecook
endif
ifeq (yes,$(have_dec_submodule))
$(dec_lib):
	$(MAKE) -C libdecnumber
endif
ifeq (yes,$(have_h3_submodule))
$(h3_lib):
	$(MAKE) -C h3
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
emain_defines      := -DDS_VER=$(ver_build)
redis_exec_defines := -DDS_VER=$(ver_build)

redis_geo_includes        = -Ih3/src/h3lib/include -I/usr/include/h3lib
redis_sortedset_includes  = -Ih3/src/h3lib/include -I/usr/include/h3lib
decimal_includes         := -Ilibdecnumber/include
ev_client_includes       := -Ilinecook/include
term_includes            := -Ilinecook/include

libraids_files := ev_net ev_service ev_http term ev_client ev_tcp ev_unix \
  ev_nats shm_client stream_buf route_db redis_msg redis_cmd_db redis_exec \
  redis_geo redis_hash redis_hyperloglog redis_key redis_list redis_pubsub \
  redis_script redis_set redis_sortedset redis_stream redis_string \
  redis_transaction decimal kv_pubsub
libraids_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(libraids_files)))
libraids_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libraids_files)))
libraids_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(libraids_files))) \
                  $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libraids_files)))
libraids_dlnk  := $(dlnk_lib)
libraids_spec  := $(version)-$(build_num)
libraids_ver   := $(major_num).$(minor_num)

$(libd)/libraids.a: $(libraids_objs)
$(libd)/libraids.so: $(libraids_dbjs)

all_libs    += $(libd)/libraids.a $(libd)/libraids.so
all_depends += $(libraids_deps)

raids_dlib  := $(libd)/libraids.so
raids_dlnk  := -L$(libd) -lraids $(dlnk_lib)

libshmdp_files := shmdp
libshmdp_dbjs  := $(addprefix $(objd)/, $(addsuffix .fpic.o, $(libshmdp_files)))
libshmdp_deps  := $(addprefix $(dependd)/, $(addsuffix .fpic.d, $(libshmdp_files)))
libshmdp_dlnk  := $(raids_dlnk)
libshmdp_libs  := $(libd)/libraids.so
libshmdp_spec  := $(version)-$(build_num)
libshmdp_ver   := $(major_num).$(minor_num)

$(libd)/libshmdp.so: $(libshmdp_dbjs) $(libshmdp_libs)

all_libs    += $(libd)/libshmdp.so
all_depends += $(libshmdp_deps)

ds_server_files      := emain
ds_server_objs       := $(addprefix $(objd)/, $(addsuffix .o, $(ds_server_files)))
ds_server_deps       := $(addprefix $(dependd)/, $(addsuffix .d, $(ds_server_files)))
ds_server_libs       := $(libd)/libraids.so
ds_server_static_lnk := $(ds_lib) $(lnk_lib) -lpcre2-32
ds_server_lnk        := $(raids_dlnk)

$(bind)/ds_server: $(ds_server_objs) $(ds_server_libs)
$(bind)/ds_server.static: $(ds_server_objs) $(ds_lib) $(lnk_lib)

all_exes    += $(bind)/ds_server $(bind)/ds_server.static
all_depends += $(ds_server_deps)

shmdp_files := smain
shmdp_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(shmdp_files)))
shmdp_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(shmdp_files)))
shmdp_libs  := $(libd)/libshmdp.so
shmdp_lnk   := -lshmdp $(raids_dlnk)

$(bind)/shmdp: $(shmdp_objs) $(shmdp_libs)

all_exes    += $(bind)/shmdp
all_depends += $(shmdp_deps)

client_files := cli
client_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(client_files)))
client_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(client_files)))
client_libs  := $(raids_dlib)
client_lnk   := $(raids_dlnk)

$(bind)/client: $(client_objs) $(client_libs)

test_msg_files := test_msg
test_msg_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_msg_files)))
test_msg_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_msg_files)))
test_msg_libs  := $(raids_dlib)
test_msg_lnk   := $(raids_dlnk)

$(bind)/test_msg: $(test_msg_objs) $(test_msg_libs)

test_cmd_files := test_cmd
test_cmd_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_cmd_files)))
test_cmd_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_cmd_files)))
test_cmd_libs  := $(raids_dlib)
test_cmd_lnk   := $(raids_dlnk)

$(bind)/test_cmd: $(test_cmd_objs) $(test_cmd_libs)

redis_cmd_files := redis_cmd redis_msg
redis_cmd_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(redis_cmd_files)))
redis_cmd_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(redis_cmd_files)))
redis_cmd_libs  := $(kv_lib)
redis_cmd_lnk   := $(kv_lib)

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

test_routes_files := test_routes
test_routes_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_routes_files)))
test_routes_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_routes_files)))
test_routes_libs  := $(raids_dlib)
test_routes_lnk   := $(raids_dlnk)

$(bind)/test_routes: $(test_routes_objs) $(test_routes_libs)

test_delta_files := test_delta
test_delta_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_delta_files)))
test_delta_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_delta_files)))
test_delta_libs  := $(raids_dlib)
test_delta_lnk   := $(raids_dlnk)

$(bind)/test_delta: $(test_delta_objs) $(test_delta_libs)

test_decimal_files := test_decimal
test_decimal_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_decimal_files)))
test_decimal_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_decimal_files)))
test_decimal_libs  := $(raids_dlib)
test_decimal_lnk   := $(raids_dlnk)

$(bind)/test_decimal: $(test_decimal_objs) $(test_decimal_libs)

test_cr_files := test_cr
test_cr_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_cr_files)))
test_cr_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_cr_files)))
test_cr_libs  :=
test_cr_lnk   :=

$(bind)/test_cr: $(test_cr_objs) $(test_cr_libs)

test_rtht_files := test_rtht
test_rtht_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_rtht_files)))
test_rtht_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_rtht_files)))
test_rtht_libs  :=
test_rtht_lnk   :=

$(bind)/test_rtht: $(test_rtht_objs) $(test_rtht_libs)

test_subht_files := test_subht
test_subht_objs  := $(addprefix $(objd)/, $(addsuffix .o, $(test_subht_files)))
test_subht_deps  := $(addprefix $(dependd)/, $(addsuffix .d, $(test_subht_files)))
test_subht_libs  := $(raids_dlib)
test_subht_lnk   := $(raids_dlnk)

$(bind)/test_subht: $(test_subht_objs) $(test_subht_libs)

all_exes    += $(bind)/client $(bind)/test_msg \
               $(bind)/redis_cmd $(bind)/test_cmd $(bind)/test_list \
	       $(bind)/test_hash $(bind)/test_set $(bind)/test_zset \
	       $(bind)/test_hllnum $(bind)/test_hllw $(bind)/test_hllsub \
	       $(bind)/test_geo $(bind)/test_routes $(bind)/test_delta \
	       $(bind)/test_decimal $(bind)/test_cr $(bind)/test_rtht \
	       $(bind)/test_subht
all_depends += $(client_deps) $(test_msg_deps) \
               $(redis_cmd_deps) $(test_cmd_deps) $(test_list_deps) \
	       $(test_hash_deps) $(test_set_deps) $(test_zset_deps) \
	       $(test_hllnum_deps) $(test_hllw_deps) $(test_hllsub_deps) \
	       $(test_geo_deps) $(test_routes_deps) $(test_delta_deps) \
	       $(test_decimal_deps) $(test_cr_deps) $(test_rtht_deps) \
	       $(test_subht_deps)

all_dirs := $(bind) $(libd) $(objd) $(dependd)

include/raids/redis_cmd.h: $(bind)/redis_cmd
	$(bind)/redis_cmd > include/raids/redis_cmd.h
gen_files += include/raids/redis_cmd.h

# the default targets
.PHONY: all
all: $(gen_files) $(all_libs) $(all_dlls) $(all_exes)

# create directories
$(dependd):
	@mkdir -p $(all_dirs)

# remove target bins, objs, depends
.PHONY: clean
clean:
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
dist_bins: $(all_libs) $(bind)/ds_server $(bind)/ds_server.static
	chrpath -d $(libd)/libraids.so
	chrpath -d $(libd)/libshmdp.so
	chrpath -d $(bind)/shmdp
	chrpath -d $(bind)/ds_server

.PHONY: dist_rpm
dist_rpm: srpm
	( cd rpmbuild && rpmbuild --define "-topdir `pwd`" -ba SPECS/raids.spec )

# dependencies made by 'make depend'
-include $(dependd)/depend.make

ifeq ($(DESTDIR),)
# 'sudo make install' puts things in /usr/local/lib, /usr/local/include
install_prefix = /usr/local
else
# debuild uses DESTDIR to put things into debian/libdecnumber/usr
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
	gcc -x c++ $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: src/%.c
	gcc $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.fpic.d: src/%.cpp
	gcc -x c++ $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.fpic.d: src/%.c
	gcc $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).fpic.o -MF $@

$(dependd)/%.d: test/%.cpp
	gcc -x c++ $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

$(dependd)/%.d: test/%.c
	gcc $(arch_cflags) $(defines) $(includes) $($(notdir $*)_includes) $($(notdir $*)_defines) -MM $< -MT $(objd)/$(*).o -MF $@

