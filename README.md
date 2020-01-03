The Problem

When we report on ad performance the customer needs a certain amount of detail to capture efficiency and differences between platforms being used.

Manually extracting the data from these different platforms is time consuming and can be error prone, therefore an automated, systematic method must be put in place to alleviate these problems.

There are currently 5 separate data sources from which data has to be extracted:

-   Google Ads
-   Google Analytics
-   Google Marketing Platform
-   Facebook
-   AdForm

All of these services have their own APIs exposed to the users for easy data extraction. Each and every one is different in design.

Inspired has a set up on AWS which has a PostgreSQL database running. This database contains data from multiple other sources already.

The Goal 

Building an ETL framework in Python to automatically extract data and store it in the Postgre DB after additional data manipulation is done. A basic diagram below shows the workflow:

![](https://storage.googleapis.com/slite-api-files-production/files/3fb836b8-b37a-4183-aa77-d21c4e72fbd5/workflow.PNG)

Code Structure

The code is structured employing the functional / procedural paradigm so that parts can be reused and to make the code more readable.

Libraries like pandas, numpy were because of their ease of use and speed in data manipulation. All other packages used are commonplace.

The code can also be found on **GitHub **at this location:

<https://github.com/justasmach/inspired>

Directory stucture is as follows:

-   inspired (main dir):
    -   helper_functions.py
    -   database.ini
    -   {platformName} (dir):
        -   {platformName}\_script.py (platform specific code)
        -   {platformName}\_conf.xlsx (platform specific cross tab)
        -   (platform specific secret file)
        -   {platformName_MM_DD_YYYY}.txt (generated log file, date distinct)
        -   (other related files)
    -   (other related files)

In detail:

1.  The API code blocks are placed in separate source files in different directories. These contain source specific code to call the API and merge the data. For more detail refer to the source code itself and the comments in it.
2.  The crosstab "conf" files contain some form of level identifiers (eg. customer ID, view ID etc.) in the _**base **_sheet_** **_while the _**requirement\* **_sheet contains dimensions, metrics and time frames. All of this is passed to the API when making the calls.
3.  Platform specific secret file contains login details for that platform.
4.  The file **helper_functions.py** contains all the functions that are used by different API code blocks specific to the service being called. Most of the functions are used by all the APIs, while some are API specific.
5.  Database.ini file contains PostgreSQL login details which are common for all APIs.
6.  Other related files (or directories) may be either GIT related or IPython instance checkpoints.

\* an exception is the Google Marketing Platform, which does not have a requirement sheet. The structure of the report and timeframes are defined and built using Google's Report Builder in the browser.

Below, a brief definition of functions in **helper_functions.py**:

| **Name**           |                                                                      **Takes**                                                                      |                          **Returns**                          |                                                                                                                                                                                                    **Description**                                                                                                                                                                                                    |
| :----------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------: | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| db_config          |                                                           filename(str)<br />section(str)                                                           |                            db(dict)                           |                                                                                                                                                                                           Reads configuration info from file                                                                                                                                                                                          |
| init_conn          |                                                                          -                                                                          |                           conn(obj)                           |                                                                                                                                                                                       Initiates a connection with PostrgreSQL DB                                                                                                                                                                                      |
| end_conn           |                                                                      conn(obj)                                                                      |                               -                               |                                                                                                                                                                                      Terminates the connection to PostrgreSQL DB                                                                                                                                                                                      |
| init_cur           |                                                                      conn(obj)                                                                      |                            cur(obj)                           |                                                                                                                                                                                                  Initiates DB cursor                                                                                                                                                                                                  |
| end_cur            |                                                                       cur(obj)                                                                      |                               -                               |                                                                                                                                                                                                    Closes DB cursor                                                                                                                                                                                                   |
| create_table       |                                                     conn(obj)<br />cur(obj)<br />tab_cr_str(str)                                                    |                               -                               |                                                                                                                                                                   Executes and commits create table statement according to the provided query string                                                                                                                                                                  |
| drop_table         |                                             conn(obj)<br />cur(obj)<br />t_name(str)<br />do_drop(bool)                                             |                               -                               |                                                                                                                                                                            Drops table with provided name if it exists and do_drop is true                                                                                                                                                                            |
| upsert             |                          conn(obj)<br />cur(obj)<br />upsert_q(str)<br />df_res_tuple (list of tuples)<br />page_size(int)                          |                               -                               |                                                                                                                                                                   Updates or inserts a batch of rows as a single execution to DB, all according to                                                                                                                                                                    |
| tab_creation_str   |                                        t_name(str)<br />pk_name(str)<br />pk_lst(list)<br />dtype_dict(dict)                                        |                         query_str(str)                        |                                                                                                                                                               Creates table if it doesn't exist already, needed only for the first run after table drop                                                                                                                                                               |
| add_missing_cl     |                                                   conn(obj)<br />cur(obj)<br /><br />str_full(str)                                                  |                               -                               |                                                                                                                                                                                 Adds missing columns when it occurs between API calls                                                                                                                                                                                 |
| get_db_cols        |                                                              cur(obj)<br />t_name(str)                                                              |                       column_names(list)                      |                                                                                                                                                                           Returns a list of columns curently present in the specified table                                                                                                                                                                           |
| pln_no_reg         |                                                                  campaign_name(str)                                                                 |                          pln_no(str)                          |                                                                                                                                                                            Returns PLN no. extracted from campaign_name column using regex                                                                                                                                                                            |
| name_cl_reg        |                                                                  campaign_name(str)                                                                 |                          name_cl(str)                         |                                                                                                                                                                         Returns clean campaign name extracted campaign_name column using regex                                                                                                                                                                        |
| rplc_nan           |                                                                   df_response(obj)                                                                  |                        df_response(obj)                       |                                                                                                                                                          Returns a modified dataframe with numpy 'nan' values replaced with 'None' which is default in Python                                                                                                                                                         |
| rename_col         |                                                                    col_name(str)                                                                    |                         col_name(str)                         |                                                                                                                                                                     Returns a modified column name with unsupported characters replaced if present                                                                                                                                                                    |
| upsert_str         |                                                       dims(str)<br />t_name(str)<br />pk(str)                                                       |                         upsert_q(str)                         |                                                                                                                                                                                              Returns a built upsert query                                                                                                                                                                                             |
| add_missing_cl_str |                                                         dtype_dict(dict)<br />db_cols(list)                                                         |                         str_full(str)                         |                                                                                                                                                                                       Returns 'add missing column' query string                                                                                                                                                                                       |
| upd_last_90        |                                                        period_xlsx(str)<br />per_format(str)                                                        | start_date(str), end_date(str)<br />OR<br />period_xlsx(list) | If the string 'upd_last_90' is found in period position in the conf excel then calculates the start and end dates to gather 90 day data from current date backwards.<br />If Google Analytics then returns two strings with start and end dates<br />If Google Ads of Facebook returns single string in according format.<br />If the aforementioned string is not present in conf excel returns the original period. |
| period_split       |                                                            def_period(dict) def_intv(int)                                                           |                        period_lst(list)                       |                                                                                                                                                                                 Returns a list of date ranges by user defined interval                                                                                                                                                                                |
| get_pln_no         |                                                  df_response(obj) src_col_name(str) is_in_df(bool)                                                  |                        df_response(obj)                       |                                                                                                                                                                   Returns a modified dataframe after PLN no. regex is applied and column is inserted                                                                                                                                                                  |
| get_name_col       |                                                  df_response(obj) src_col_name(str) is_in_df(bool)                                                  |                        df_response(obj)                       |                                                                                                                                                                Returns a modified dataframe after campaign name regex is applied and column is inserted                                                                                                                                                               |
| add_ts             |                                                                   df_response(obj)                                                                  |                        df_response(obj)                       |                                                                                                                                                            Returns a modified dataframe with current timestamps inserted as creation_ts and last_updated_ts                                                                                                                                                           |
| get_types          |                                                                   df_response(obj)                                                                  |                        col_dtypes(dict)                       |                                                                                                          Assigns data type for each column based on the values in the column. As it can be tricky, there are exceptions defined as well. Returns a dictionary with each value containing column and datatype.                                                                                                         |
| log_string         |                                                             platform(str)<br />text(str)                                                            |                               -                               |                                                                                                                                                                                Logs received string to file based on specified platform                                                                                                                                                                               |
| postgre_write_main | df_response(obj) t_name(str)<br />pk_name(str)<br />pk_lst(list)<br />do_drop(bool)<br />page_size(int)<br />src_col_name(str)<br />is_pln_df(bool) |                               -                               |                                                                                                                                                                          Main function that runs other functions to write data to PostgreSQL                                                                                                                                                                          |

All relevant information regarding service APIs is located in the following links:

Google Ads: <https://developers.google.com/google-ads/api/docs/start>

Google Analytics: <https://developers.google.com/analytics/devguides/reporting/core/v4>

Google Marketing Platform: <https://developers.google.com/doubleclick-advertisers/v3.3/>

Facebook: <https://developers.facebook.com/docs/marketing-apis/>

AdForm: <https://api.adform.com/help/references/buyer-solutions/reporting>

Logging

The code has logging built into it. A log file is appended to every time the code is run but it's distinct by platform and by day, not by run, meaning that only a single file will be generated per platform, per day no matter how many times the code is run. The Python _Logging _library was not used, because there was simply no need for it's extended functionality.

Database

The following tables are either created or most likely inserted into when the code is run:

-   google_ads_new
-   google_analytics_new
-   google_marketing_platform_new
-   facebook_marketing_new
-   facebook_marketing_conv_new
-   adform_cadreon_new
-   adform_reprise_new

All of them have very different columns and primary keys, which could also change in the future.

Database table configuration is done in the code itself, but is mostly standard accross all platform scripts.

Use cases

_**Reporting automation**_

All of the source code is uploaded to Windows 2012 server and saved in the _Documents _folder under the _reporting_ username. The same file structure is kept. Automation is achieved by using the built in Windows Task Scheduler. All of the code is run every night at 01:00 AM (unless changed).

All scripts are defined to run with the following configuration, which is as seen in the pictures:

![](https://storage.googleapis.com/slite-api-files-production/files/c07d508d-e7b5-4067-a288-24514d0851c6/Untitled1.png)

![](https://storage.googleapis.com/slite-api-files-production/files/55029f21-5091-4522-8b38-2ef341699a94/Untitled2.png)

![](https://storage.googleapis.com/slite-api-files-production/files/5bc87475-3128-4cd5-803b-5838ffc7d77e/Untitled3.png)

_**Custom reports**_

If needed the excel file can be configured as preferred (or in cases like with Google Marketing Platform one would use the Report Builder instead of the excel file). One can change period, dimensions, metrics etc. In most cases this should work without any code changes, but some of the platforms have limitations of what kinds of dimension/metric combinations can be requested for a single data entry (for ex. Facebook). Also, there could be some undefined technical shortcomings of the source code, for ex. some dimension names can include unsupported characters (database column name constraints) that were not met in the development stage.

If a completely custom report is required, changes in the source code must be made to modify the database configuration (table name, primary key etc.) ortherwise an error will occur trying to insert incompatible data into the standard reporting tables.
