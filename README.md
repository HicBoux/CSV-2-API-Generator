<h1>CSV 2 API Generator</h1>

Simply and automatically generate APIs from your CSVs. Built in Python with Flask Restx.

<h2>Features</h2>

The application generates automatically API endpoints from the CSV stored in the ./data directory.<br>
Generated Endpoints <b>that can be enabled or disabled</b> :
<ul>
    <li>REST GET:</li>
    <ul>
      <li>ALL_DATA : Get the whole CSV file stored, as a 'records' style oriented JSON.</li>
      <li>HEADER : Get the header of the CSV file stored.</li>
      <li>FILTER : Filter on an input value of an input column and get the whole CSV file stored, as a 'records' style oriented JSON.</li>
      <li>SUMMARY_STATS : Get summary statistics describing the CSV file stored as a 'columns' style oriented JSON.</li>
      <li>VALUE_COUNTS : For each column CSV file stored, get the count of records by each possible value found.</li>
      <li>SQL : Get the result of a SQL query on the CSV file stored.</li>
    </ul>
    <li>REST POST</li>
    <ul>
      <li>SEARCH : Search & filter over the CSV file stored to only retrieved the filtered sub data as a 'records" style oriented JSON.</li>
      <li>CSV_FILE_CREATION : Create and store a new CSV file in the directory from an input JSON.</li>
    </ul>
    <li>REST PUT</li>
    <ul>
      <li>ROW_APPEND : Append new row(s) to an existing CSV file stored.</li>
      <li>VALUE_REPLACE : Replace some values filtered in a CSV file by other ones.</li>
    </ul>
    <li>REST DELETE</li>
    <ul>
      <li>ROW_DELETION : Delete some rows from the existing CSV file stored.</li>
      <li>COLUMN_DELETION : Delete the column in the input URL from the stored CSV file.</li>
      <li>CSV_FILE_DELETION : Delete from storage the whole CSV file in the input URL.</li>
   </ul>
</ul>

<h2>How to set it :</h2>

1) Set the config files in ./config/config.json as wished:</br>
```json
{
    "cipher_key":"7uV$b9xc10_3mS|8",
    "activated_endpoints":["all_data","header","filter","summary_stats","value_counts", "sql", "search", "csv_file_deletion", "row_deletion", "column_deletion", "csv_file_deletion","row_append","value_replace"],
    "pandas_chunksize":300 
}
```
<ul>
<li>cipher_key : choose the cipher key to encrypt/decrypt SQL queries sent in the URL.
  <ul>
  <li>The encryption method is base64.urlsafe_b64encode / base64.urlsafe_b64decode.</li>
  <li>The method is shown in the main of the code.</li>
  </ul>
</li>
<li>activated_endpoints : choose which endpoints have to be enabled. By default all posibles ones are enabled.</li>
<li>pandas_chunksize : the pandas chunksize used when reading CSV files. It avoids memory bugs due to a too large file loaded in "one shot".</li>
</ul>

2) Open a terminal and change your current directory to the repository's root one and execute :</br>
-Simply with the makefile: ```make```</br>
-With a docker command : ```docker build -t csv2api .``` and ```docker run -it -p 5000:5000 csv2api```</br>

3) If it's the first time, wait a few minutes so that the app gets installed. Otherwise, it should start in a few seconds.

4) Enjoy :) ! If you're running on localhost and your csv is named "your_csv_name.csv", try http://localhost:5000/csv2api/your_csv_name/header, it should work !

5) If necessary, it is possible to remove/uninstall the whole app/docker image from the disk with the following command : ```make stop``` and then ```make remove```

<h2>API Documentation</h2>

An automated and interactive documentation is generated at the root URL. By default, it should be accessible on http://localhost:5000/. You can even try the API with your web browser thanks to this online documentation.

<h2>Credits :</h2>

Copyright (c) 2021, HicBoux. Work released under MIT License. 

(Please contact me if you wish to use my work in specific conditions not allowed automatically by the MIT License.)

