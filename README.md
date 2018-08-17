Personal Work - Milestone 1 and Milestone 2 tags

# 5300-Ladybug

This is the final implmentation of SQL5300 for the final two milestones for the summer term of CSPC5300
Here we implment a basic DBMS system in c++. Runnin this program does take some know how, so here is what to do:

To run a program linked against our private Berkeley DB library, you have to export LD_LIBRARY_PATH. So do this (you can put it in your ~/.bash_profile, without the leading $ prompt):

$ export LD_LIBRARY_PATH=/usr/local/db6/lib:$LD_LIBRARY_PATH

You can build the example using the Makefile:
$ make

You can clear the build by using the command
$ make clean

Then you can run the program that's been linked against the Berkeley DB software library. The program reqires a r/w directory path as a command line agruemnt which is where the DB will be constructed ie:

$ ./sql5300 /home/st/mouserj/cpsc5300/data

this will result in a SQL prompt. You can query SQL Statments as per usual but the only some queries are supported, these are:
SHOW TABLES
SHOW COLUMNS FROM
CREATE TABLE
CREATE INDEX
DELETE FROM
INSERT INTO
DROP TABLE
DROP INDEX

In order to exit the program just type "quit"

In order to test our storage engine functionality as well as the btree.cpp implmentation simply type "test"

It may be necessary to clear the data folder of existing db files if they already exist. As seen in the test, one of the tables is created regardless of the check if it exists, so this is likely. In this case, it may be wise to include in the makefile:

$ rm -rf ~/cpsc5300/data/*

This will remove the db files and allows one to run the test again.
