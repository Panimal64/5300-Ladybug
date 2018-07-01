#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;


class executeSQL {
	//Function to create database environment
	DbEnv *_DB_ENV;

	bool inital = false;

	void initialize_environment(char *envHome) {

		const char *home = std::getenv(envHome);

		DbEnv env(0U);
		env.set_message_stream(&std::cout);
		env.set_error_stream(&std::cerr);
		env.open(envHome, DB_CREATE|DB_INIT_MPOOL,0);

		Db db(&env,0);
		inital = true;
	}

	// Gets SQL statement and determines what kind
	// of statement it is.
	void getExpression(const Expr* express) {

		switch(express-> type) {
			case kExprStar:
				cout << "*";
				break;
			case kExprColumnRef:
				if (express->table != NULL) {
					cout << express->table << ".";
				}
				cout << express->name; 
				break;
			case kExprOperator:
				getExpression(express->expr);
				cout << " " << express->opChar << " ";
				getExpression(express->expr2);
				break;
			case kExprLiteralFloat:
				cout << express->fval;
				break;
			case kExprLiteralInt:
				cout << express->ival;
				break;
			case kExprLiteralString:
				cout << express->name;
				break;
			default:
				cout << "/nInvalid expression" << endl;
				return;
		}

		if (express->alias != NULL) {
			cout << "AS ";
			cout << express->alias << " "; 
		}
	}

	// Gets table information
	void tableInfo(TableRef * tInfo) {

		switch(tInfo->type) {
			case kTableName:
				cout << tInfo->name << " ";
				break;
			case kTableJoin:
				determineJoin(tInfo->join);
				break;
			case kTableCrossProduct:
				for(TableRef* tbl : *tInfo->list){
					tableInfo(tbl);
				}
				break;
			default:
				cout << "Invalid Table" << endl;
				break;
		}

		if(tInfo->alias != NULL){
			cout << "AS ";
			cout << tInfo->alias << " ";
		}
	}

	// Determines the type of JOIN
	void determineJoin(JoinDefinition *jDef) {

		tableInfo(jDef->left);

		switch(jDef->type){
			case kJoinInner: 
				cout << "INNER JOIN ";
				break;
			case kJoinOuter:
				cout << "OUTER JOIN ";
				break;
			case kJoinLeft:
				cout << "LEFT JOIN ";
				break;
			case kJoinRight: 
				cout << "RIGHT JOIN ";
				break;
			case kJoinLeftOuter: 
				cout << "LEFT OUTER JOIN ";
				break;
			case kJoinRightOuter:
				cout << "RIGHT OUTER JOIN ";
				break;
			case kJoinCross: 
				cout << "JOIN CROSS";
				break;
			case kJoinNatural: 
				cout << "NATURAL JOIN ";
				break;
			default: 
				break;
		}

		tableInfo(jDef->right);
		cout << "ON ";

		getExpression(jDef->condition);
		cout << " ";
	}

	// Executes SELECT statements
	void exSelect(const SelectStatement* sel_stmt) {

			int size = (*sel_stmt->selectList).size();
			int i = 0;

			cout << "SELECT ";

			for (Expr* expr : *sel_stmt->selectList) {
				getExpression(expr);

				i++;
				
				if(i < size) cout << ", ";
			}

			cout << " FROM ";
			tableInfo(sel_stmt->fromTable);

			if(sel_stmt->whereClause != NULL) {
				cout << "WHERE ";
				getExpression(sel_stmt->whereClause->expr);

				cout << " " << sel_stmt->whereClause->opChar << " ";
				getExpression(sel_stmt->whereClause->expr2);
			}
		}

	// Executes CREATE statements
	void exCreate(const CreateStatement* cre_stmt) {

		int size = (*cre_stmt->columns).size();
		int i = 0;

		cout << "CREATE ";

		switch(cre_stmt->type) {
			case CreateStatement::kTable: 
				cout << "TABLE " << cre_stmt->tableName << " (";

				for (ColumnDefinition* c_def : *cre_stmt->columns) {

					cout << *c_def->name;

					switch(c_def->type)	{
						case ColumnDefinition::UNKNOWN: 
							cout << " UNKNOWN";
							break;
						case ColumnDefinition::TEXT: 
							cout << " TEXT";
							break;
						case ColumnDefinition::INT: 
							cout << " INT";
							break;
						case ColumnDefinition::DOUBLE: 
							cout << " DOUBLE";
							break;
						default: ;
					}

					i++;

					if (i < size) {
						cout << ", ";
					}
				}
				cout << ")";
				break;
			default: 
				cout << "/nInvalid create statement" << endl;
				break;
		}
	}

	// Identifies query type of SQL statement and selects the corresponding 
	// fucntion to process the query. Only CREATE and SELECT statements 
	// are working.
	void exQuery(const SQLStatement * stmt) {

		switch(stmt->type()) {
			case kStmtSelect:
				exSelect((const SelectStatement*)stmt);
				break;
			case kStmtCreate:
				exCreate((const CreateStatement*)stmt);
				break;
			default:
				break;
		}
		cout << endl;
	}

	// Creates DB and begins getting user input
	public:
		void runExecuteSQL(char* path){
			// Note on how to exit program
			cout << "Enter 'quit' to exit program\n";

			if(!inital){
				initialize_environment(path);
			}

			// While loop to take in SQL queries until user types quit
			while(true){

				string query;

				cout << endl;
				cout << "SQL>>";

				getline(cin, query);

				// Parses SQL statement
				SQLParserResult *parse = SQLParser::parseSQLString(query);

				// Exit if user enters quit or print SQL statement from parser
				// if statement is valid, otherwise let user know of invalid
				// SQL statement.
				if(query == "quit") {
					return ;
				} else if(parse -> isValid()) {
					exQuery(parse->getStatement(0));
				} else {
					cout << "Invalid Statement" << endl;	
				}
			}
		}
};


int main (int argc, char *argv[]) {

	// String to hold user query
	string query;  

	// Checks to see if user provided file path argument, if not exits program
	if (argc !=2){
		cout << endl;
		cerr << "Please provide write file path argument\n";
		exit(1);
	}

	char* path = argv[1];

	executeSQL sql;
	// Run SQL Parser
	sql.runExecuteSQL(path);

} 


