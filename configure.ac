dnl Process this file with autoconf to produce a configure script.

define([scm_r], esyscmd([sh -c "git rev-parse --short=7 HEAD"])) dnl
define([coredirect_ver_major], 1)
define([coredirect_ver_minor], 0)
define([ts], esyscmd([sh -c "date +%Y%m%d%H%M%S"])) dnl
define([revcount], esyscmd([git rev-list HEAD | wc -l | sed -e 's/ *//g' | xargs -n1 printf])) dnl

AC_INIT([coredirect], [coredirect_ver_major.coredirect_ver_minor])

AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR(config)
AC_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE([1.10 foreign tar-ustar silent-rules subdir-objects])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

dnl Checks for programs
AC_PROG_CC
AC_PROG_CXX
AC_GNU_SOURCE
AC_PROG_LN_S
AC_PROG_LIBTOOL

##########################
# Enable tests
#
AC_ARG_ENABLE(
    [test],
    [AC_HELP_STRING([--enable-test],
                   [Build test programs (default=no)])],
    [enable_test=$enableval],
    [enable_test=yes])
AM_CONDITIONAL(TEST_ENABLE, test x$enable_test = xyes)

##########################
# Enable support for valgrind
#
AC_ARG_WITH([valgrind],
    AC_HELP_STRING([--with-valgrind],
        [Enable Valgrind annotations (small runtime overhead, default NO)]))
if test x$with_valgrind = x || test x$with_valgrind = xno; then
    want_valgrind=no
    AC_DEFINE([NVALGRIND], 1, [Define to 1 to disable Valgrind annotations.])
else
    want_valgrind=yes
    if test -d $with_valgrind; then
        CPPFLAGS="$CPPFLAGS -I$with_valgrind/include"
    fi
fi


dnl Checks for libraries
AC_CHECK_LIB([ibverbs], [ibv_get_device_list], [], [AC_MSG_ERROR([libibverbs not found])])

AC_CHECK_LIB(dl, dlsym, [],
    AC_MSG_ERROR([dlsym() not found.  coredirect requires libdl.]))

AC_CHECK_LIB(pthread, pthread_mutex_init, [],
    AC_MSG_ERROR([pthread_mutex_init() not found.  coredirect requires libpthread.]))

dnl Checks for header files.
AC_HEADER_STDC

if test x$want_valgrind = xyes; then
AC_CHECK_HEADER(valgrind/memcheck.h,
    [AC_DEFINE(HAVE_VALGRIND_MEMCHECK_H, 1,
        [Define to 1 if you have the <valgrind/memcheck.h> header file.])],
    [if test $want_valgrind = yes; then
        AC_MSG_ERROR([Valgrind memcheck support requested, but <valgrind/memcheck.h> not found.])
    fi])
fi

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST


AC_CONFIG_FILES([Makefile])
AC_OUTPUT
