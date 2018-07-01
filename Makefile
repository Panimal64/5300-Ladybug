MS1: MS1.o
	g++ -L/usr/local/berkeleydb/lib -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

MS1.o : MS1.cpp 
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<

clean:
	rm -rf *.o MS1