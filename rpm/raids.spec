Name:		raids
Version:	999.999
Vendor:	        Rai Technology, Inc
Release:	99999%{?dist}
Summary:	Rai distribution services

License:	ASL 2.0
URL:		https://github.com/raitechnology/%{name}
Source0:	%{name}-%{version}-99999.tar.gz
BuildRoot:	${_tmppath}
Prefix:	        /usr
BuildRequires:  gcc-c++
BuildRequires:  chrpath
BuildRequires:  raikv
BuildRequires:  raimd
BuildRequires:  rdbparser
BuildRequires:  h3lib
BuildRequires:  libdecnumber
BuildRequires:  linecook
BuildRequires:  pcre2-devel
BuildRequires:  git-core
BuildRequires:  liblzf-devel
BuildRequires:  systemd
BuildRequires:  c-ares-devel
BuildRequires:  openssl-devel
BuildRequires:  zlib-devel
Requires:       raikv
Requires:       raimd
Requires:       rdbparser
Requires:       h3lib
Requires:       libdecnumber
Requires:       linecook
Requires:       pcre2
Requires:       liblzf
Requires:       c-ares
Requires:       openssl
Requires:       zlib
Requires(post): /sbin/ldconfig
Requires(postun): /sbin/ldconfig

%description
Distribution services for a raikv, a shared memory key value store.

%prep
%setup -q


%define _unpackaged_files_terminate_build 0
%define _missing_doc_files_terminate_build 0
%define _missing_build_ids_terminate_build 0
%define _include_gdb_index 1

%build
make build_dir=./usr %{?_smp_mflags} dist_bins
cp -a ./include ./usr/include
mkdir -p ./usr/share/doc/%{name}
cp -a ./README.md graph doc console ./usr/share/doc/%{name}/

%install
rm -rf %{buildroot}
mkdir -p  %{buildroot}

# in builddir
cp -a * %{buildroot}

install -p -D -m 644 ./config/limit.conf %{buildroot}%{_sysconfdir}/systemd/system/%{name}.service.d/limit.conf
sed -e 's:/usr/bin:%{_bindir}:;s:/var:%{_localstatedir}:;s:/etc:%{_sysconfdir}:;s:/usr/libexec:%{_libexecdir}:' \
     ./config/%{name}.service > tmp_file
install -p -D -m 644 tmp_file %{buildroot}%{_unitdir}/%{name}.service
install -p -d -m 755 %{buildroot}%{_mandir}/man1/
install -p -D -m 644 ./doc/*.1 %{buildroot}%{_mandir}/man1/

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_prefix}/lib64/*
%{_includedir}/*
%doc %{_docdir}/*
%doc %{_mandir}/*
%config(noreplace) %{_sysconfdir}/systemd/system/%{name}.service.d/limit.conf
%config(noreplace) %{_unitdir}/%{name}.service

%pre
getent group %{name} &> /dev/null || \
groupadd -r %{name} &> /dev/null
getent passwd %{name} &> /dev/null || \
useradd -r -g %{name} -d %{_sharedstatedir}/%{name} -s /sbin/nologin -c "Rai KV" %{name}
exit 0

%post
echo "%{_prefix}/lib64" > /etc/ld.so.conf.d/%{name}.conf
/sbin/ldconfig
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
if [ $1 -eq 0 ] ; then
rm -f /etc/ld.so.conf.d/%{name}.conf
fi
/sbin/ldconfig

%changelog
* Sat Jan 01 2000 <support@raitechnology.com>
- Hello world
