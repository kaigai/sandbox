#!/bin/sh
#
AUDITCTL=`which 2>/dev/null || echo /sbin/auditctl`
AUSEARCH=`which 2>/dev/null || echo /sbin/ausearch`
ARCH="b32"; uname -m | grep -q 64 && ARCH="b64"

#
# Internal command: dump_as_tree <aufiled> <pid> <prefix>
#
dump_as_tree()
{
    local AUFILE="$1"
    local PID="$2"
    local PREFIX="$3"
    local CHILD=""

    # Dump the given entry
    LINE1=`$AUSEARCH -if $AUFILE --pid $PID -sc exit_group 2>/dev/null | grep '^type=SYSCALL ' | tail -1`
    test $? -eq 0 || return;
    CMD=`echo $LINE1 | grep -Eo ' exe=\"[^\"]+\" ' | grep -Eo '\"[^\"]+\"'`
    DOMAIN=`echo $LINE1 | grep -Eo ' subj=[^ ]+' | sed 's/:/ /g' | awk '{ print $3}'`
    UNAME=`echo $LINE1 | grep -Eo ' euid=[0-9]+ ' | grep -Eo '[0-9]+'`
    UNAME=`getent passwd $UNAME | sed 's/:/ /g' | awk '{print $1}'`
    ARGS="(????)"

    LINE2=`$AUSEARCH -if $AUFILE --pid $PID -sc execve 2>/dev/null | grep '^type=EXECVE ' | tail -1`
    if [ $? -eq 0 ]; then
	if echo "$LINE2" | grep ' argc=0 '; then
	    ARGS=""
	else
	    ARGS=`echo "$LINE2" | grep -Eo 'a[1-9][0-9]*=\".*\"' | sed 's/a[0-9]\+=//g'`
	fi
    fi
    echo "${PREFIX}- ${CMD} ${ARGS} (pid=${PID}, user=${UNAME}, domain=${DOMAIN})"

    for CHILD in `$AUSEARCH -if $AUFILE -pp $PID -sc exit_group 2>/dev/null | grep '^type=SYSCALL ' | grep -Eo ' pid=[0-9]+ ' | grep -Eo '[0-9]+'`; do
	dump_as_tree $AUFILE $CHILD "  $PREFIX"
    done
}

if echo "$1" | grep -Eq '^[0-9]+$'; then
    if [ `id -u` -ne 0 ]; then
	echo "`basename $0`: should run as root"
	exit 1
    fi
    TEMPFILE=`mktemp`
    $AUSEARCH --raw -ul "$1" > $TEMPFILE
    ROOTPID=`$AUSEARCH -if $TEMPFILE -m LOGIN | grep '^type=LOGIN' | grep -Eo ' pid=[0-9]+' | grep -Eo '[0-9]+' | tail -1`
    for ROOT_PID in `$AUSEARCH -if $TEMPFILE --ppid $ROOTPID -sc exit_group | grep '^type=SYSCALL ' | grep -Eo ' pid=[0-9]+ ' | grep -Eo '[0-9]+'`
    do
	dump_as_tree "$TEMPFILE" "$ROOT_PID" ""
    done
#    rm -f $TEMPFILE
    exit 0
fi

#
# Guidance
#
AUID=$RANDOM

echo "usage:"
echo "  Do the following set up, and run: `basename $0` $AUID"
echo
echo "  $AUDITCTL -D"
echo "  $AUDITCTL -a exit,always -F arch=$ARCH -S exit_group -F auid=$AUID"
echo "  $AUDITCTL -a exit,always -F arch=$ARCH -S execve -F auid=$AUID"
echo "  echo $AUID > /proc/\$\$/loginuid"

exit 0