# sql_sysbench_proc
Slow Query Log parser for interact with Sysbench as an independent process throw a LUA script

# Compiling

gcc sysbench_slow_proc.c `pkg-config --libs glib-2.0 --cflags glib-2.0` -o sysbench_slow_proc 

