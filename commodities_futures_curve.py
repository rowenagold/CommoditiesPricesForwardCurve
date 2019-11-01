
from threading import current_thread
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

"""
@author: BernardR
@summary:
This program takes commodity future contract prices data from the 'input table' ,
 processes the data and finally builds a table which contains daily date-ranges
 ( between contract DeliveryStart and DeliveryEnd)
 of the future contracts prices recursively.
    1. determine if current contracts are active i.e volume>0
       a. if monthly contractType, 
                 if all month in present quarter is active, append MonthPrice

       b. if quarterly contract, 
                 if all quarters in present YEAR is active, append YearPrice
                     2. append to newTable, (dateIndex) DateRange between DeliveryStart and DeliveryEnd of contract

    newTable which contains daily date-range of all commodity contracts is stored in output table:  
    

"""


@DecorateErrorHandling
def getTablesName():
    tbl_dict = {'commodities_prices': tb.commodities_prices_table(tb.SYNCED)}
    return tbl_dict


@DecorateErrorHandling
def getsqldata(tbl_dict):
    """

    :param tbl_dict: Table name to retrieve all commodities contact prices
    :return: dataframe of all commodities contract price, as specified in SQL parameters
    """
    # retrieve data from last working week day
    sql_last_bd = ''' SELECT *
            FROM {tb_name} 
            #WHERE DATE(utcTimeStamp) = "2019-10-23"
           WHERE DATE(utcTimeStamp) = (CASE WEEKDAY(CURRENT_DATE)      
           WHEN 0 THEN SUBDATE(CURRENT_DATE,3)
            WHEN 6 THEN SUBDATE(CURRENT_DATE,2) 
            ELSE SUBDATE(CURRENT_DATE,1) END)
            ORDER BY commodity, utcTimeStamp, deliveryStart '''
    sql_last_download = """ 
                     SELECT * FROM {tb_name}
                WHERE DATE(utcTimeStamp) = ( select MAX(DATE(a.utcTimeStamp)) 
                FROM {tb_name} a
                WHERE a.price IS NOT NULL)"""
    results = getValuesFromTable(sql_last_bd.format(tb_name=tb.commodities_prices_table(tb.SYNCED)))
    if len(results) == 0:    #if download is not synced
        results = getValuesFromTable(sql_last_download.format(tb_name=tb.commodities_prices_table(tb.SYNCED)))
    columnNames = ["commodity", "market", "exchange", "currency", "unit", "contractType", "contractName",
                   "utcTimeStamp", "locTimeStamp", "price",  "open", "high", "low",  "oi", "volume", "deliveryStart",
                   "deliveryEnd"]

    df = pd.DataFrame.from_records(list(results), columns=columnNames)
    df = df.assign(days=0, month=0, quarter=0, season='', year=0)
    df['deliveryStart'] = pd.to_datetime(df.deliveryStart, format="%Y-%m-%d", exact=True)
    df['deliveryEnd'] = pd.to_datetime(df.deliveryEnd, format="%Y-%m-%d",exact=True)
    return df


def fill_quarterly_values(month):
    """
    :param month: Commodities contract month values, 1-January, 2-February
    :return: return quarter values corresponding to month[ QTR1-Jan,Feb, Mar], QTR2-Apr,May,June] etc
    """
    if month in [1, 2, 3]:
        return 1
    elif month in [4, 5, 6]:
        return 2
    elif month in [7, 8, 9]:
        return 3
    elif month in [10, 11, 12]:
        return 4


@DecorateErrorHandling
def contract_type(df):
    """

    :param df: dataframe of commodities contract prices
    :return: features engineered for better parsing of data, Month, season, or year
    """
    if (str(df).lower().startswith('d')) or (str(df).lower().startswith('f')) :
        return 'a_day'
    elif (str(df).lower().startswith('w')):
        if (str(df).lower().endswith('k')):
            return 'c_week'
        else:
            return 'b_weekend'
    elif str(df).lower().startswith('m'):
        return 'd_month'
    elif str(df).lower().startswith('q'):
        return 'e_quarter'
    elif str(df).lower().startswith('s'):
        return 'f_season'
    elif str(df).lower().startswith('y'):
        return 'g_year'

@DecorateErrorHandling
def fill_month_quarter_values(df):
    """

    :param df: dataframe of commodities contract prices
    :return: dataframe of commodities contract prices with additional columns using several self-made functions for easier data parsing
    """
    df['days'] = [d.days + 1 for d in df.deliveryEnd - df.deliveryStart]
    df['month'] = df['deliveryStart'].dt.month
    df['quarter'] = df['month'].apply(fill_quarterly_values)
    df['year'] = df['deliveryStart'].dt.year
    df['season'] = [('winter' + str(x.year)) if x.month in [10, 11, 12, 1, 2, 3] else 'summer' + str(x.year) for x in df['deliveryStart'].dropna()]
    df['contract'] = df['contractType'].apply(contract_type)
    return df


def build_date_index(df):
    """

    :param df: subset dataframe of commodities contract prices
    :return: working dataframe with index=daterange corresponding to commodities contract in input df
    """
    #Builds the working (empty) dataframe with the index which are the date ranges between DeliveryStart and DeliveryEnd
    start_value = sorted(list(df.deliveryStart.dropna()))[0]
    end_value = sorted(list(df.deliveryEnd.dropna()))[-1]
    column_range = pd.date_range(start=start_value, end=end_value).strftime("%Y-%m-%d")
    column = ['dateIndex'] + list(df.columns)
    newTable = pd.DataFrame(columns=column,index=column_range)
    return newTable

@DecorateErrorHandling
def append_date_index(df, newTable):
    """

    :param df: subset dataframe of commodities contract prices
    :param newTable: working dataframe with index=daterange corresponding to commodities contract in input df
    :return: updated working dataframe

    appends row values after subseequent subsetting of dataset by specific market and exchange
    """

    for indx in list(df.index):
        date_range = pd.date_range(start=df.loc[indx, 'deliveryStart'], end=df.loc[indx, 'deliveryEnd']).strftime(
            "%Y-%m-%d")
        for dates in date_range:
            newTable.loc[dates] = [str(dates)] + list((df.loc[indx]))
    return newTable

@DecorateErrorHandling
def append_country(df):
    """

    :param df: dataframe of commodities contract prices
    :return: dataframe of commodities contract prices with column[country] to indicate contract hub
    """
    df['country'] = 'country'
    return df

@DecorateErrorHandling
def clean_data(df):
    """

    :param df: dataframe of commodities contract prices
    :return: cleaned dataframe of commodities contract prices, with various data wrangling methods carried out
    """
    df.dropna(subset=['deliveryStart', 'deliveryEnd', 'volume', 'price'], inplace=True)
    df.drop_duplicates(['commodity', 'market', 'exchange', 'deliveryStart', 'deliveryEnd'], keep='last', inplace=True)
    df['deliveryStart'] = pd.to_datetime(df.deliveryStart, format="%Y-%m-%d", exact=True)
    df['deliveryEnd'] = pd.to_datetime(df.deliveryEnd, format="%Y-%m-%d", exact=True)
    df.drop(df[df.deliveryStart.dt.year < pd.datetime.today().year].index, inplace=True)
    df['deliveryStart'] = df.deliveryStart.dt.strftime("%Y-%m-%d")
    df['deliveryEnd'] = df.deliveryEnd.dt.strftime("%Y-%m-%d")
    df = df.applymap(lambda s: s.lower() if type(s) == str else s)
    return df


@DecorateErrorHandling
def append_value(date_range, newTable, df_subset, indx):
    """
         :param date_range: date-range between deliveryStart and deliveryEnd for current subset of contracts
  :param newTable: working dataframe
  :param df_subset: current subset of contracts
  :param indx: index of current subset of contracts
  :return:
  """

    if newTable is not None:
        for dates in date_range:
            if not newTable.loc[dates].isnull().all():
                continue
            newTable.loc[dates] = [str(dates)] + list((df_subset.loc[indx]))
        return newTable
    else:
        return newTable

@DecorateErrorHandling
def create_mixed_curve(df):
    """

    :param df: dataframe of commodities contract prices
    :return: Final dataframe of commodities contract prices forward curve.
            This will contain multiple contracts concatenated to build a forward curve
            with daterange between first and last available contract type for each commodity
    """
    df = clean_data(df)
    column = ['dateIndex'] + list(df.columns)
    final_table = pd.DataFrame(columns=column,)

    for commodity_name in list(set(df.commodity)):  # Subsetting dataframe by commodity

        commodity_df = df[df.commodity == commodity_name ]

        for market_name in list(set(commodity_df.market)):  # Subsetting specific commodity_dataframe by market
            commodity_market_df = commodity_df[commodity_df.market == market_name]

            for exchange_name in list(set(commodity_market_df.exchange)):  # Subsetting specific commodity_market_ dataframe by exchange
                commodity_market_exchange_df = commodity_market_df[commodity_market_df.exchange == exchange_name]

                newTable = build_date_index(commodity_market_exchange_df)
                for df_year in list(set(commodity_market_exchange_df.year)):  # Subsetting specific commodity_market_exchange dataframe by year
                    df_subset = commodity_market_exchange_df[commodity_market_exchange_df.year == df_year]  # df_subset contains each possible commodity_market_exchange_year combination,

                    df_subset.sort_values(by=[ 'contract', 'deliveryEnd','deliveryStart',], inplace= True, )

                    for indx in list(df_subset.index):

                        month_ = df_subset.loc[indx, 'month']
                        quarter_ = df_subset.loc[indx, 'quarter']
                        year_ = df_subset.loc[indx, 'year']
                        season_ = df_subset.loc[indx, 'season']
                        contract = df_subset.loc[indx, 'contract']

                        months_in_quarter = df_subset[(df_subset.index != indx) & (df_subset.quarter == quarter_) & (df_subset.year == year_) & (df_subset.contract != 'd_quarter')]# & (df_subset.contract == 'C-Month')]  # .loc[indx, 'contractType'] not in ["SEASON", "QUARTER"])]
                        quarter_in_year = df_subset[(df_subset.index != indx)  & (df_subset.contract == 'd_quarter')]
                        months_in_year = df_subset[(df_subset.index != indx) & (df_subset.year == year_)]
                        season_in_year = df_subset[(df_subset.index != indx) & (df_subset.season == season_) & (df_subset.contract != 'f_year')]
                        date_range = pd.date_range(start=df_subset.loc[indx, 'deliveryStart'], end=df_subset.loc[indx, 'deliveryEnd']).strftime("%Y-%m-%d")

                        if contract == 'a_day':
                            newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                       # weekend contract
                        elif contract == 'b_weekend':
                            newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                        # weekly contract
                        elif contract == 'c_week':
                            newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                # Monthly contract
                        elif contract == 'd_month':
                            if df_subset.loc[indx, 'volume'] != 0:
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif df_subset.loc[indx,'volume'] == 0: #if current contract not active?
                                if any(x for x in ['e_quarter','f_season', 'g_year'] if x in set(months_in_quarter['contract'])): #if any longer contracttype active
                                    continue
                                else:
                                    newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 in list(months_in_quarter['volume']): #all monthly contract active?
                                if any(x for x in ['e_quarter', 'f_season', 'g_year'] if x in set(months_in_year['contract'])): #if any longer contracttype active
                                    continue
                                else:
                                    newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif not list(months_in_quarter['volume']) or not set(months_in_quarter['contract']): #all quarterly contract active or available?
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 not in list(months_in_quarter['volume']):
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                        # Quarterly Contract
                        elif contract == 'e_quarter':
                            if df_subset.loc[indx,'volume'] == 0 and any(x for x in ['f_season', 'g_year'] if x in set(months_in_year['contract'])):
                                continue

                            elif not list(months_in_quarter['volume']):  #all monthly contract active?
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 not in list(months_in_quarter['volume']): #all quarterly contract active?
                                continue

                            elif 0 in list(months_in_quarter['volume']) or len(list(months_in_quarter['volume'])) != 0: #all quarterly contract active or available?
                                if 0 in list(quarter_in_year['volume']):
                                    if any(x for x in ['f_season', 'g_year'] if x in set(df_subset['contract'])):
                                        continue
                                else:
                                    newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                        #Season contracts
                        elif contract == 'f_season':
                            if df_subset.loc[indx,'volume'] == 0 and 'g_year' in set(months_in_year['contract']):
                                continue

                            elif not list(months_in_quarter['volume']):
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 not in list(months_in_quarter['volume']):  #all monthly contract active?
                                continue

                            elif 0 not in list(quarter_in_year['volume']):  #all quarterly contract active?
                                continue

                            elif 0 not in list(season_in_year['volume']): #all season contract active?
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 in list(season_in_year['volume']):
                                if 'g_year' not in list(df_subset['contract']):
                                    newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe
                                else:
                                    continue

                            elif len(list(months_in_quarter['volume'])) == 0:
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            else:
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe


                    # Yearly
                        elif contract == 'g_year':
                            if 0 in list(season_in_year['volume']) or not list(season_in_year['volume']) : #all season contract active or available?
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                            elif 0 not in list(months_in_quarter['volume']):  #all monthly contract active?
                                continue

                            elif 0 not in list(quarter_in_year['volume']): # all quarterly contract active?
                                continue

                            elif 0 not in list(season_in_year['volume']): # all season contract active?
                                continue

                            else:
                                newTable = append_value(date_range, newTable, df_subset, indx)  # append to working dataframe

                newTable['dateIndex'] = newTable.index
                final_table = pd.concat([final_table,newTable]).drop_duplicates()
    #Putting it all together and filling out dates with no contracts
    final_table.fillna(method='ffill', inplace=True, )
    final_table['contract'] = [str(d).split('_')[-1] for d in final_table.contract]
    final_table['contractType'] = final_table.contract
    final_table['curve_type'] = 'mixed_curve'
    return final_table

@DecorateErrorHandling
def create_single_curves(df):
    """
    :param df: dataframe of commodities contract prices
    :return: Final dataframe of commodities contract prices forward curve.
            This will contain forward curves for each contractype available
            with daterange between first and last available contract for each commodity
    """

    final_table = pd.DataFrame()        #Initializing final output table

    for commodity_name in list(set(df.commodity)):  # Subsetting dataframe by commodity
        commodity_df = df[df.commodity == commodity_name]

        for market_name in list(set(commodity_df.market)):  # Subsetting specific commodity_dataframe by market
            commodity_market_df = commodity_df[commodity_df.market == market_name]

            for exchange_name in list(set(commodity_market_df.exchange)):  # Subsetting specific commodity_market_ dataframe by exchange
                commodity_market_exchange_df = commodity_market_df[commodity_market_df.exchange == exchange_name]

                for df_contract_type in list(set(commodity_market_exchange_df.contract)):  # Subsetting specific commodity_market_exchange dataframe by year
                    df_ = commodity_market_exchange_df[commodity_market_exchange_df.contract == df_contract_type]  # dff contains each possible commodity_market_exchange_year combination,

                    #Subset of contracts with non-zero volumes
                    if (df_contract_type  in ['a_day', 'b_weekend', 'c_week']) or not any(list(df_.volume)):
                         df_ =df_
                    else:
                        non_zero = np.max(np.nonzero(list(df_.volume))) #Selecting contracts with non-zero volumes
                        if (non_zero == 0):
                            continue
                        else:
                            df_ = df_.iloc[:non_zero+1, ]

                    newTable = build_date_index(df_)
                    newTable = append_date_index(df_, newTable)
                    newTable.fillna(method='ffill', inplace=True, )
                    newTable['dateIndex'] = newTable.index
                    final_table = pd.concat([final_table, newTable])

    final_table['contract'] = [str(d).split('_')[-1] for d in final_table.contract]
    final_table['contractType'] = final_table.contract
    final_table['curve_type'] = 'single_curve'
    return final_table


@DecorateErrorHandling
def insertValuetoSQL(df):
    """
    :param df: commodities contract prices forward curve
    :return:
    """
    df_list = df.values.tolist()
    columns_name = df.columns
    insertValuesIntoTable(tb.commodity_price_forward_curve_table(tb.UNSYNCED), columns_name, df_list)
    return


@DecorateErrorHandling
def runMainFunction():
    """
    a pipeline to parse data through functions created.
    Finally appends to the database the forward curves for each commodity
    :return:
    """
    runTimeController = current_thread().getRunTimeController()
    logger = runTimeController.logger.getLogger()
    tbl_dict = getTablesName()
    df = getsqldata(tbl_dict)

    df.dropna(subset=['deliveryStart', 'deliveryEnd'], inplace=True)
    df = fill_month_quarter_values(df) #creating fields for better parsing of data
    df = clean_data(df)
    #df.to_csv(r"C:\workspace\pycharm_projects\trunk\Paula_Python\src\Carbon\PersonalFolder\BernardR\test_data.csv",index=None, header=True)
    single_curve = create_single_curves(df)
    mixed_curve = create_mixed_curve(df)
    single_curve.rename(columns={'dateIndex': 'utcTimeStamp',
                       'utcTimeStamp': 'utcTradeDate',
                       'contractType': 'contractType1',
                       'contractName': 'contractType2'}, inplace=True)
    mixed_curve.rename(columns={'dateIndex': 'utcTimeStamp',
                                 'utcTimeStamp': 'utcTradeDate',
                                 'contractType': 'contractType1',
                                 'contractName': 'contractType2'}, inplace=True)
    single_curve.drop(['days', 'month', 'quarter', 'season', 'year', 'contract', 'locTimeStamp', 'deliveryStart', 'deliveryEnd'],
            inplace=True, axis=1, errors='ignore')  # Dropping modified fields
    mixed_curve.drop(
        ['days', 'month', 'quarter', 'season', 'year', 'contract', 'locTimeStamp', 'deliveryStart', 'deliveryEnd'],
        inplace=True, axis=1, errors='ignore')
    insertValuetoSQL(single_curve)
    insertValuetoSQL(mixed_curve)
    logger.info('All data from tblpr commodity has been finished')


@DecorateErrorHandling
def starttest():
    debugMode = True
    displayLogsOnConsole = True
    insertLogsIntoDatabase = False
    runTimeController1 = RunTimeController("commodity_prices_forward_curve", debugMode, insertLogsIntoDatabase,
                                           displayLogsOnConsole)
    t1 = CustomThread(runTimeController=runTimeController1, target=runMainFunction, args=())
    t1.start()


if __name__ == "__main__":
    starttest()


