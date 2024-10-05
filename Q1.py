import psycopg2
import pandas as pd

# Database connection details
db_config = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

try:
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Query table and convert to pandas df
    query_trades = "SELECT * FROM trades;"
    df_trades = pd.read_sql(query_trades, conn)

    query_user = "SELECT * FROM users;"
    df_user = pd.read_sql(query_user, conn)
    # df = pd.read_csv('trades.csv')

# findings-----------------------------------------------------------------------------------
    # 1. get max and min of each column, volume column has 0 values. Filter all rows with zero volume.
    print('\n1. volume column with 0 values')
    agg = df_trades.agg(['min','max'])
    print(agg)
    zero_volume = df_trades[df_trades['volume'] == 0]
    print(zero_volume)

    # 2. Check if null values exist. Found 7 rows with null contractsize. Get all COFFEE rows to see if the nulls can be replaced with values from other rows of symbol COFFEE. However, all COFFEEs are with null contractsize.
    print('\n2.Null values')
    null_values = df_trades[df_trades.isnull().any(axis=1)]
    print(null_values)
    COFFEE = df_trades[df_trades['symbol'] == 'COFFEE']

    # 3. Check if duplicate exists in the users table
    dup = df_user.duplicated().sum()
    print(f'\n3.Number of duplicated rows in the users table: {dup}')

    # 4. Check if login_hash in the trades table exist in the users table
    users_not_found = df_trades[~df_trades['login_hash'].isin(df_user['login_hash'])]
    print(f'\n4.login_hash in the trades table that cannot be found in the users table')
    print(users_not_found)

# Other checks without findings-------------------------------------------------------------
    # Get statstics summary for numeric fields
    des = df_trades.describe()
    print('\nStatstics summary for numeric fields')
    print(des)

    # Check if duplicate exists in the trades table
    dup = df_trades.duplicated().sum()

    # get unique values in the symbol, cmd and digits columns to check if unexpected values exist
    df_group = df_trades.groupby([ 'symbol']).size()
    df_group = df_trades.groupby([ 'cmd']).size()
    df_group = df_trades.groupby([ 'digits']).size()

    # Check if close time is always later than open time
    df_trades['time_diff'] = pd.to_datetime(df_trades['close_time']) - pd.to_datetime(df_trades['open_time'])
    f = df_trades[df_trades['time_diff']<pd.Timedelta(0)]

    # Check if all hashed fields are of equal length
    df_trades['login_hash_len'] = df_trades['login_hash'].str.len()
    df_group = df_trades.groupby([ 'login_hash_len']).size()
    df_trades['ticket_hash_len'] = df_trades['ticket_hash'].str.len()
    df_group = df_trades.groupby([ 'ticket_hash_len']).size()
    df_trades['server_hash_len'] = df_trades['server_hash'].str.len()
    df_group = df_trades.groupby([ 'server_hash_len']).size()

    # Check if unexpected contract size exists
    df_group = df_trades.groupby([ 'contractsize']).size()
    # print(df_group)

except Exception as e:
    print(f"An error occurred: {e}")
