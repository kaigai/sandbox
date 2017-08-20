nstream: kds_src.o kds_dst.o kgscan.o prog.o nstream.c
	$(CC) -I /usr/local/cuda/include    \
          -L /usr/local/cuda/lib64      \
          -lcuda -lpthread $^ -o $@
kds_src.c:  kds_src.gz
	gunzip -c kds_src.gz > kds_src.c
kds_src.o:	kds_src.c
	$(CC) -c kds_src.c
kds_dst.o:	kds_dst.c
	$(CC) -c kds_dst.c
kgscan.o: kgscan.c
	$(CC) -c kgscan.c
prog.o: prog.c
	$(CC) -c prog.c
clean:
	rm kds_src.o kds_dst.o kgscan.o prog.o nstream kds_src.c

