/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#inlcude "ParseTreeToString.h"
#include "schema_tables.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
	// FIXME
}
QueryResult::~QueryResult(string query) {
    string message = query;

}

//Executes Query
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    if(tables != nullptr){
        SQLExec::tables = new Tables();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//get the type of column being described, and store data identifier and attribute back to passed address
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    //set identifier as statement col's name
    column_name = col->name;
    //get col's type. set column attribute to match statement col
    switch(col->type){
        case ColumnAttribute::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnAttribute::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            out << "???";
    }
    throw SQLExecError("not implemented");  // FIXME
}

//Creates a table based on input statement
// REF(https://github.com/hyrise/sql-parser/blob/master/src/sql/CreateStatement.h)
QueryResult *SQLExec::create(const CreateStatement *statement) {

    //Variables
    Identifier tableID;
    Identifier column_name;
    ColumnAttribute column_attribute;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ValueDict row;
    std::string message;
    //get statement info
    tableID= statement->tableName;
    for(ColumnDefinition *col: *statement->columns){
        column_definition(*col,column_name, column_attribute ); //append statment to var
        column_names.push_back(column_name); //append to list of column names
        column_attributes.push_back(column_attribute);
    }
    //create a row, get handles
    row["table_name"]= tableID;
    Handle table_handle = SQLExec::tables->insert(&row);

    //create column handle an
    try {
        Handles column_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {

            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                column_handles.push_back(columns.insert(&row));
            }

            // Create Relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception& e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: column_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(table_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("Created " + table_name);
}



// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
	return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	return new QueryResult("not implemented"); // FIXME
}

