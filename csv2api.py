from flask import Flask, jsonify, request
from flask_restx import Api, Resource, reqparse, fields
import pandas as pd
import pandasql as ps
from pandas.api.types import is_numeric_dtype
import os
import json
import base64
import re

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
api = Api(app,
    title='CSV 2 API',
    version='1.0',
    description='A simple API generated automatically with the stored CSV files in ./data'
)

home_directory = "./data/"
API_error_no_CSV_found = "API successfully reached but this CSV was not found. Please try another endpoint with an existing filename."
API_error_endpoint_disabled = "API successfully reached but this functionnality of the API / endpoint is disabled."

def convert_pandas_dtypes_to_builtin_types(col_type):
    """Convert pandas data types to python builtin ones, like pandas object to python str."""
    col_type = str(col_type)
    if "str" in col_type:
        return str
    elif "int" in col_type:
        return int
    elif "float" in col_type:
        return float
    elif "bool" in col_type:
        return bool
    else:
        return str

def read_csv_by_chunks(complete_path, chunksize):
    """Read & load progressively a stored CSV file chunk by chunk."""
    df_chunk = pd.read_csv(complete_path, chunksize=chunksize)
    chunk_list = []
    for chunk in df_chunk:
        chunk_list.append(chunk)
    df_concat = pd.concat(chunk_list)
    return df_concat


# Definition of the API namespace 'csv2api' which will be called. API general documentation is at '/'
ns_csv2api = api.namespace('csv2api', 
                description = "CSV files stored in the /data/ directory and to load in order to build the API.")

# Explanation of the general body query dictionary expected by POST/PUT methods : 
# -> The API user will have to write instead into a JSON dictionary each column and its value which will be used as filter, new row etc...
general_query_dict_model = api.model('General CSV Query Dictionary Model', {
    'column1': fields.String(
        description='The name in the CSV of the column1 and its value1 (as numeric, boolean or string) which will be used as body query of the PUT/POST method.'),
    'column2': fields.String(
        description='The name in the CSV of the column2 and its value2 (as numeric, boolean  or string) which will be used as body query of the PUT/POST method.'),
    'column3': fields.Integer(
        description='The name in the CSV of the column3 and its value3 (as numeric, boolean  or string) which will be used as body query of the PUT/POST method.'),
    'column4': fields.Boolean(
        description='The name in the CSV of the column4 and its value4 (as numeric, boolean  or string) which will be used as body query of the PUT/POST method.'),
    'etc': fields.String(
        description='etc... for all other wished columns. Columns:values are not mandatory, zero to infinity can be sent to the server. '),
})
class Query_Dict(fields.Raw):
    def format(self, value):
        return {'column1': value.column1, 'column2': value.column, "etc...":value.etc}

# -----------------------------
# GET --- Restful API HTTP Verb
# -----------------------------

@ns_csv2api.route('/<filename>/all_data')
class CSV_all_data(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename):
        """REST GET : Get the whole CSV file stored, as a 'records' style oriented JSON."""
        if "all_data" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))
                return json.loads(df.to_json(orient="records")), 200
            else:
                return {"Message": API_error_no_CSV_found }, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/header')
class CSV_header(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename):
        """REST GET : Get the header of the CSV file stored."""
        if "header" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))
                df_header = {}
                for c in df.columns:
                    df_header[c] = str(df.dtypes[c])
                return jsonify(df_header)
            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/filter/<queried_column>/<queried_value>')
class CSV_filter(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename, queried_column, queried_value):
        """REST GET : Filter on an input value of an input column and get the whole CSV file stored, \
            as a 'records' style oriented JSON."""
        if "filter" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                # Retrieve data according to the filter (numeric or textual)
                if is_numeric_dtype(df[queried_column]):
                    filtered_df = df[df[queried_column] == float(queried_value)]
                else:
                    filtered_df = df[df[queried_column] == str(queried_value)]

                return json.loads(filtered_df.to_json(orient="records")), 200

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/summary_stats')
class CSV_summary_stats(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename):
        """REST GET : Get summary statistics describing the CSV file stored as a 'columns' style oriented JSON."""
        if "summary_stats" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                return json.loads(df.describe().to_json(orient="columns")), 200

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

@ns_csv2api.route('/<filename>/value_counts')
class CSV_value_counts(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename):
        """REST GET : For each column CSV file stored, get the count of records by each possible value found."""
        if "value_counts" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                value_counts = {}
                for col in df.columns:
                    value_counts[col] = json.loads(df[col].value_counts().to_json(orient="columns")), 200

                return jsonify(value_counts)

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/sql')
class CSV_sql(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but this CSV was not found in the SQL query nor in the URL.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def get(self, filename):
        """REST GET : Get the result of a SQL query on the CSV file stored."""
        if "sql" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                # Get the JSON body of the API REST query
                parser = reqparse.RequestParser()
                parser.add_argument("query", type=str, required=True)
                # Get the input parameter 'query' which contains the crypted SQL query to apply on the CSV/dataframe
                query = str(parser.parse_args()["query"])
                decoded_query = base64.urlsafe_b64decode(query.encode()).decode()

                # Insensitive case value replacement
                insensitive_subst = re.compile(re.escape('from ' + filename), re.IGNORECASE)
                decoded_query = insensitive_subst.sub('FROM df', decoded_query)

                # Query the CSV/dataframe and return the result, or return an error message
                if 'FROM df' in decoded_query:
                    query_df = ps.sqldf(decoded_query)
                    del df
                    return json.loads(query_df.to_json(orient="records")), 200
                else:
                    return {"Message": str(
                        "API successfully reached but this CSV was not found in the SQL query."
                        + "Please try to rewrite your query and make sure that your 'FROM xxx' statement is right.")}, 404

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

# -----------------------------
# POST --- Restful API HTTP Verb
# -----------------------------

@ns_csv2api.route('/<filename>/search')
class CSV_search(Resource):
    @api.doc(responses={
        200:'Success',
        404:'API successfully reached but no filter or CSV were found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    @api.expect(general_query_dict_model, validate=False, required=False)
    def post(self, filename):
        """REST POST : Search & filter over the CSV file stored to only retrieved the filtered sub data \
            as a 'records" style oriented JSON."""
        if "search" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                #Parse the URL
                parser = reqparse.RequestParser()
                for c in df.columns:
                    t = convert_pandas_dtypes_to_builtin_types(df.dtypes[c])
                    parser.add_argument(c, type=t)
    
                #Retrieve the filters in the URL and query the DF
                query_dict = parser.parse_args()
                filtered_df = df.copy()
                for k in query_dict.keys():
                    if query_dict[k] != None:
                        filtered_df = filtered_df[filtered_df[k] == query_dict[k]]

                #Return the filtered DF or a success message if no filter has been sent
                if filtered_df.equals(df):
                    return {"Message":"API successfully reached but no filter (which at least filter some rows) was found. \
                        Please add it or go to ./all_data to get the whole CSV."}, 404
                else:
                    return json.loads(filtered_df.to_json(orient="records")), 200

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/csv_file_creation/<orient>')
class CSV_file_creation(Resource):
    @api.doc(responses={
        202:'Success',
        417:'API successfully reached but an error occured while creating the file.',
        404:'API successfully reached but this CSV already exists.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    @api.expect(general_query_dict_model, validate=False, required=False)
    def post(self, filename, orient):
        """REST POST : Create and store a new CSV file in the directory from an input JSON."""
        if "csv_file_deletion" in activated_endpoints:
            if not os.path.isfile(home_directory + filename + ".csv"):

                # Transform input rows of the JSON body of the API REST query into dataframe
                query_dict = request.get_json()
                if orient in ["split", "records", "index", "columns", "values"]:
                    new_df = pd.read_json(json.dumps(query_dict), orient=orient)
                else:
                    new_df = pd.read_json(json.dumps(query_dict), orient='index')

                # Save results or return an error message
                try:
                    new_df.to_csv(home_directory + filename + ".csv", index = False)
                    return {"Message":"API successfully reached and the CSV was well created in the directory."}, 200
                except:
                    return {"Message":"API successfully reached but an error occured while creating the file."}, 417

            else:
                return {"Message":"API successfully reached but this CSV already exists. \
                    Please try another endpoint with a NONE existing filename."}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

# -----------------------------
# PUT --- Restful API HTTP Verb
# -----------------------------

@ns_csv2api.route('/<filename>/row_append/<orient>')
class CSV_new_row(Resource):
    @api.doc(responses={
        202:'Success : API successfully reached and the CSV was well updated in the directory.',
        400:'API successfully reached but the JSON was bad interpreted.',
        409:'API successfully reached but an error occured while creating the file.',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    @api.expect(general_query_dict_model, validate=False, required=False)
    def put(self, filename, orient):
        """REST PUT : Append new row(s) to an existing CSV file stored."""
        if "row_append" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):

                # Original CSV file
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))
                if "0" in df.columns:
                    df = df.drop("0", 1) # Delete the index_col created named "0"

                # Transform input rows of the JSON body of the API REST query into dataframe
                query_dict = request.get_json()
                print(json.dumps(query_dict))
                if orient == "records":
                    new_rows_df = pd.read_json(json.dumps(query_dict), orient=orient)
                elif orient in ["split", "records", "index", "columns", "values"]:
                    new_rows_df = pd.read_json(json.dumps(query_dict), orient=orient)
                else:
                    new_rows_df = pd.read_json(json.dumps(query_dict), orient='index')

                # Return 202 if row(s) well appended or 409 otherwise
                print(df.head())
                print(new_rows_df.head())
                if list(df.columns) == list(new_rows_df.columns):
                    try:
                        updated_df = df.append(new_rows_df)
                        updated_df.to_csv(home_directory + filename + ".csv", index = False)
                        return {"Message":"API successfully reached and the CSV was well updated in the directory."}, 202
                    except:
                        return {"Message":"API successfully reached but an error occured while creating the file."}, 409
                else:
                    return {"Message":"API successfully reached but the JSON was bad interpreted. \
                        Maybe change the orientin the URL like row_append/orient."}, 400
            else:
                return {"Message":"API successfully reached but this CSV was not found. "}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405


@ns_csv2api.route('/<filename>/value_replace')
class CSV_value_replace(Resource):
    @api.doc(
        params={
        '_column_to_update_': 'the name of the column to update/change',
        '_new_value_to_set_': 'the new value to insert instead of the old one'
        },
        responses={
        202:'Success : API successfully reached and CSV file modified.',
        409:'API successfully reached but no update done.',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
        })
    @api.expect(general_query_dict_model, validate=False, required=False)
    def put(self, filename):
        """REST PUT : Replace some values filtered in a CSV file by other ones."""
        if "value_replace" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                # Parse the URL
                parser = reqparse.RequestParser()
                parser.add_argument("_column_to_update_", required=True)
                parser.add_argument("_new_value_to_set_", required=True)
                for c in df.columns:
                    t = convert_pandas_dtypes_to_builtin_types(df.dtypes[c])
                    parser.add_argument(c, type=t)

                # Retrieve the filters in the URL and apply them
                query_dict = parser.parse_args()
                column_to_update = query_dict["_column_to_update_"]
                new_value_to_set = query_dict["_new_value_to_set_"]
                filtered_df = df.copy()
                for k in query_dict.keys():
                    if query_dict[k] != None and k not in ["_column_to_update_", "_new_value_to_set_"]:
                        filtered_df = df[filtered_df[k] == query_dict[k]]

                # Replace chosen/filtered values by new ones through a mapping
                new_df = df.copy()
                new_map_to_replace = {}
                for i in filtered_df.index:
                    print(i)
                    new_map_to_replace[ new_df.loc[i][column_to_update] ] = new_value_to_set
                new_df[column_to_update].replace(new_map_to_replace, inplace=True)
                print(new_df)

                # Return the 304 status if the values have been well replaced or 202 otherwise
                if not new_df.equals(df):
                    new_df.to_csv(home_directory + filename + ".csv", index = False)
                    return {"Message":"API successfully reached and CSV file modified."}, 202
                else:

                    return {"Message":"API successfully reached but no update done."}, 409

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

# -----------------------------
# DELETE --- Restful API HTTP Verb
# -----------------------------

@ns_csv2api.route('/<filename>/row_deletion')
class CSV_row_deletion(Resource):
    @api.doc(responses={
        200:'Success',
        304:'API successfully reached but CSV row(s) not found.',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def delete(self, filename):
        """REST DELETE : Delete some rows from the existing CSV file stored."""
        if "row_deletion" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))


                # Parse the URL
                parser = reqparse.RequestParser()
                for c in df.columns:
                    t = convert_pandas_dtypes_to_builtin_types(df.dtypes[c])
                    parser.add_argument(c, type=t)

                # Retrieve the filters in the URL and query the DF
                query_dict = parser.parse_args()
                filtered_df = df.copy()
                for k in query_dict.keys():
                    if query_dict[k] != None:
                        filtered_df = filtered_df[filtered_df[k] != query_dict[k]]

                # Return the 304 status if the row has been well removed or 202 if no row has been found
                if filtered_df.equals(df):
                    return {"Message":"API successfully reached but CSV row(s) not found."}, 304
                else:
                    filtered_df.to_csv(home_directory + filename + ".csv", index = False)
                    return {"Message":"API successfully reached and CSV row(s) deleted."}, 202

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

@ns_csv2api.route('/<filename>/column_deletion/<column_name>')
class CSV_column_deletion(Resource):
    @api.doc(responses={
        200:'Success',
        304:'API successfully reached but CSV column not found.',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def delete(self, filename, column_name):
        """REST DELETE : Delete the column in the input URL from the stored CSV file."""
        if "column_deletion" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                df = read_csv_by_chunks(home_directory + filename + ".csv", int(config["pandas_chunksize"]))

                # Drop the input column in the URL
                if column_name in list(df.columns):
                    filtered_df = df.drop(str(column_name), axis=1)
                else:
                    filtered_df = df.copy()

                # Return the 304 status if the column has been well removed or 202 if no column has been found
                if filtered_df.equals(df):
                    return {"Message":"API successfully reached but CSV column not found."}, 304
                else:
                    filtered_df.to_csv(home_directory + filename + ".csv", index = False)
                    return {"Message":"API successfully reached and CSV column deleted."}, 202

            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

@ns_csv2api.route('/<filename>/csv_file_deletion')
class CSV_file_deletion(Resource):
    @api.doc(responses={
        200:'Success',
        409:'API successfully reached but no CSV deleted.',
        404:'API successfully reached but this CSV was not found.',
        405:'API successfully reached but this functionnality of the API / endpoint is disabled.'
    })
    def delete(self, filename):
        """REST DELETE : Delete from storage the whole CSV file in the input URL."""
        if "csv_file_deletion" in activated_endpoints:
            if os.path.isfile(home_directory + filename + ".csv"):
                os.remove(home_directory + filename + ".csv")
                if os.path.isfile(home_directory + filename + ".csv"):
                    return {"Message":"API successfully reached but no CSV deleted."}, 409
                else:
                    return {"Message":"API successfully reached and CSV completely deleted from the directory."}, 200
            else:
                return {"Message":API_error_no_CSV_found}, 404
        else:
            return {"Message":API_error_endpoint_disabled}, 405

if __name__ == '__main__':

    # Load input config parameters
    config = json.load(open('./config/config.json',))
    activated_endpoints = config["activated_endpoints"]
    cipher_key = config["cipher_key"]

    # Example of encryption / decryption of sql query that can be put in an URL
    # encoded = base64.urlsafe_b64encode("""SELECT * FROM my_csv WHERE attribute = 'A' """.encode()).decode()
    # print(encoded)
    # decoded = base64.urlsafe_b64decode(encoded).decode()
    # print(decoded)

    app.run(debug=False, host='0.0.0.0', port=5000)
    