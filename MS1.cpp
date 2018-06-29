#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;


//Function to create database environment
DbEnv *_DB_ENV;
void initialize_environment(char *envHome) {

	const char *home = std::getenv(envHome);

	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envHome, DB_CREATE|DB_INIT_MPOOL,0);

	Db db(&env,0);
}


int main (int argc, char *argv[]) {
	
	string query;  //String to hold user query

	//Checks to see if user provided file path argument, if not exits program
	if (argc !=2){
		cout << endl;
		cerr << "Please provide write file path argument\n";
		exit(1);
	}

	//Initialize Database environment
	initialize_environment(argv[1]);

	//Note on how to exit program
	cout << "Enter 'quit' to exit program\n";

	//While loop to take in SQL queries until user types quit
	while(true){
		cout << endl;
		cout << "SQL>>";
		getline(cin, query);
	//	cout << query;

		hsql::SQLParserResult *parse = hsql::SQLParser::parseSQLString(query);

		if(query == "quit") {
			return -1;
		} else if(parse -> isValid()) {
			cout << "VALID" << endl;

			for(uint i = 0; i < parse->size(); i++){

//				hsql::printStatementInfo(parse->getStatement(i));

				cout << hsql::parse->getStatement(i);

			}

		} else {
			cout << "Invalid Statement" << endl;	
		}


		
	}

} 


