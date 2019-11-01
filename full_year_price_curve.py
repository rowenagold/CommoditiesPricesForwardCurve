
from threading import current_thread
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import re
from calendar import monthrange
warnings.filterwarnings("ignore")

"""
@author: BernardR
@summary:         
        1.  This script takes the historic data (as specified in the sql queries) and merges it to the forward data, 
        The forward data is retrieved using this script:  
        and forward data table is found here: {commodity_price_forward_curve_table}
        
        2.  These two data-frames are concatenated together, and interpolation is done to fill out empty rows
        
        3.  For the relevant commodities, conversion to specific Currency (ex. usd-euro) and Metric (barrel – mw/h) is done.
        
        4.  Finally, the price curve for the full year in view, is exported to the table. {: commodities_price_curve_table}
 
"""

#Year in which to consider the full price curve. Setting it as a global variable
years = '19'
yearstamp = '2019'



def get_historical_data(sql):
    """
    :param sql:  SQL query for retrieving data
    :return:    data as dataframe
    """
    global years
    global yearstamp  #set year and yearstam as global value for sql query
    sql = sql.format(years=years, yearstamp=yearstamp)
    results = getValuesFromTable(sql.format(commodities_prices_table=commodities_prices_table(tb.SYNCED)))
    columnNames = ['commodity', 'market', 'contractName', 'utcTimeStamp', "price"]
    df = pd.DataFrame.from_records(list(results), columns=columnNames)
    return df


def set_date_time(timestamp,):
    """
    :param timestamp: This is a short function specific for Brent data. Brent contract are retrieved for 2 months before, then the contractName(string)
                        has to be used to get contract date period
    :return: list of datetime values, gotten from string-like dates
    """
    global yearstamp
    date_ = re.split('(\d+)', timestamp)
    whole_str = ' '.join((date_[0], yearstamp))
    date_ = datetime.strptime(whole_str, '%B %Y')
    date__ = str(date_.year) + '-' + str(date_.month) + '-' + str(monthrange(date_.year, date_.month)[1])
    return  date__

def build_date_index(df,end_value):
    """
    :param df: Dataframe
    :param end_value: End Date for working dataframe. This end_value is last date from historical data available
    :return: Returns working dataframe with index as DataRange for a full curve
    """
    global yearstamp
    start_value, end_value = pd.to_datetime(yearstamp + '01' + '01', infer_datetime_format=True),pd.to_datetime((end_value), infer_datetime_format=True)
    column_range = pd.date_range(start=start_value, end=end_value) #.strftime("%Y-%m-%d")
    columnNames = df.columns
    newTable = pd.DataFrame(columns=columnNames, index=column_range)
    return newTable


def append_rows(df, newTable):
    """
    :param df:  Dataframe
    :param newTable:  Working dataframe with full DateRange index
    :return: return a dataframe with rows appended from df into newTable, and then finally fill blank rows
    """
    for index, row in df.iterrows():
        newTable.loc[index] =list(row)
    newTable=newTable.fillna(method='ffill').fillna(method='bfill')
    return newTable



def get_forward_data(commodity, market, maxUtcTimeStamp):
    """

    :param commodity: commodity name to retrieve data ex. 'carbon'
    :param market: market name to retrieve data ex. 'eua'
    :param maxUtcTimeStamp: TimeStamp to retrieve data. This timestamp is the max date from historical data already downloaded
    :return: FORWARD contracts data as dataframe
    """
    global yearstamp

    # retrieve data from last working week day
    sql = '''SELECT commodity, market, contractType1 as 'contractName', utcTimeStamp, price 
                        FROM {tb_name}
                        WHERE commodity = '{commodity}'
                        AND market= '{market}'
                        AND curve_type = 'mixed_curve'                        
                        AND DATE(utcTimeStamp) > DATE('{maxUtcTimeStamp}') 
                        
                        #the next line retrieves forward data until last day of YEARSTAMP, if all available contracts are required, comment this line
                        AND DATE(utcTimeStamp) <   CONCAT({yearstamp},'-', '12', '-', '31')+ interval 1 DAY
                          
                        group by utcTimeStamp                       
                       '''
    results = getValuesFromTable(sql.format(commodity=commodity,  market=market, maxUtcTimeStamp=maxUtcTimeStamp, yearstamp=yearstamp, tb_name=commodities_prices_table(tb.SYNCED)))
    columnNames = ['commodity', 'market', 'contractName', 'utcTimeStamp', "price"]
    df = pd.DataFrame.from_records(list(results), columns=columnNames)
    return df

@DecorateErrorHandling
def getexchange(exchange_rate_sql):
    """
    :param exchange_rate_sql: SQL query to retrieve exchange rates for period in GLOBAL TIMESTAMP
    :return: exchange rates for period in GLOBAL TIMESTAMP
    """
    global yearstamp
    sql = exchange_rate_sql.format(yearstamp=yearstamp)
    results = getValuesFromTable(sql.format(tblout_foreign_exchange_rate=tb.tblout_foreign_exchange_rate(tb.SYNCED),
                                            tblout_exchange_rate_meta=tb.tblout_exchange_rate_meta(tb.SYNCED)))
    columnNames = ["startlocTimeStamp", "fromCurrency", "toCurrency", "rate"]
    df = pd.DataFrame.from_records(list(results), columns=columnNames)
    df.index = df['startlocTimeStamp']
    exchange_df = build_date_index(df,'2019-12-31')
    full_exchange_df = append_rows(df, exchange_df)
    full_exchange_df['utcTimeStamp'] = full_exchange_df.index
    return full_exchange_df


def rate_conversion(commodity_data, exchange_data, metric_change,metrics_name ):
    """
    :param commodity_data:       Complete dataframe with historical and forward data concatenated together
    :param  exchange_data:      Complete (for whol year range) dataframe of exchange rate values.
    :param metric_change:       Metric value for conversion ex. 8.141 for barrel_to_MW/h ,
    :param metrics_name:        barrel_to_MW/h
    :return:                    Dataframe which contains columns with relevant converted price
    """
    metric = metric_change
    commodity_data['metric_change'] = metric
    commodity_data['price_usd_per_mwh'] = commodity_data['price']/metric
    commodity_data['exchange_rate'] = [np.float(rate) for rate in exchange_data['rate']]
    commodity_data['price_euro_per_mwh'] = commodity_data['price_usd_per_mwh'] * commodity_data['exchange_rate']
    commodity_data['currency_change'] = exchange_data['fromCurrency'] + '_' + exchange_data['toCurrency']
    commodity_data['metrics_name'] = metrics_name
    commodity_data['modelrunDate'] = pd.datetime.now().date()
    return commodity_data


@DecorateErrorHandling
def insertValuetoSQL(df):
    """
    :param df: commodities contract prices forward curve
    :return:
    """
    df_list = df.values.tolist()
    columns_name = df.columns
    insertValuesIntoTable(tb.commodities_prices_table(UNSYNCED), columns_name, df_list)
    return



brent_sql = '''
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'-1
                AND MONTH(utctimestamp)= 11
                AND a.contractName=concat("january",'{years}') 
            union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'-1
                AND MONTH(utctimestamp)= 12
                AND a.contractName=concat("february",'{years}') 

            union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 1
                AND a.contractName=concat("march",'{years}') 

                union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 2
                AND a.contractName=concat("April",'{years}')        

                    union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 3
                AND a.contractName=concat("may",'{years}') 

                    union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 4
                AND a.contractName=concat("june",'{years}')     

                        union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" 
            from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 5
                AND a.contractName=concat("july",'{years}')     

                            union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 6
                AND a.contractName=concat("august",'{years}')   

                                union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 7
                AND a.contractName=concat("september",'{years}') 

                                    union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 8
                AND a.contractName=concat("october",'{years}') 

                                        union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 9
                AND a.contractName=concat("november",'{years}') 

                                            union   
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,round(avg(price),2) AS "price" from tblpr_commodities_prices a 
                WHERE commodity="Brent" 
                AND market='IPE e-Brent' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)= 10
                AND a.contractName=concat("december",'{years}') '''

gas_sql = """
            SELECT commodity, market,contractName,utcTimeStamp,price   
            FROM tblpr_commodities_prices
            WHERE commodity="gas" 
            AND contractname="day-ahead" 
            AND YEAR(utctimestamp)='{yearstamp}'
            AND market='ttf'  """


coal_sql = """
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=1
                AND a.contractName=concat("January",'{years}') 
                
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=2
                AND a.contractName=concat("february",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=3
                AND a.contractName=concat("march",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=4
                AND a.contractName=concat("april",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=5
                AND a.contractName=concat("may",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=6 
                AND a.contractName=concat("june",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=7
                AND a.contractName=concat("july",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=8
                AND a.contractName=concat("august",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}' 
                AND MONTH(utctimestamp)=9
                AND a.contractName=concat("september",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=10
                AND a.contractName=concat("october",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}' 
                AND MONTH(utctimestamp)=11
                AND a.contractName=concat("november",'{years}') 
            UNION 
            SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price AS "price" from tblpr_commodities_prices a 
                WHERE commodity="coal" 
                AND market='api2' 
                AND YEAR(utctimestamp)='{yearstamp}'
                AND MONTH(utctimestamp)=12
                AND a.contractName=concat("december",'{years}')  """



carbon_sql = """ SELECT a.commodity, a.market,a.contractName,a.utcTimeStamp,price 
                    from tblpr_commodities_prices a 
                        WHERE commodity="Carbon" 
                        AND market='EUA' 
                        and YEAR(utctimestamp)='{yearstamp}'
                    #   and MONTH(utctimestamp)=1
                       and a.contractName='Daily TP3' 
                    #   AND a.contractName=concat("January",'{years}') """

exchange_rate_sql = """ select t1.startLocTimeStamp, t2.fromCurrency, t2.toCurrency , t1.rate
                    from tblout_foreign_exchange_rate_data t1
                    inner join tblout_foreign_exchange_rate_meta t2
                    using(sourceID)
                    where t2.fromCurrency='USD'
                    and t2.toCurrency='EUR'
                    and t2.timeInterval='daily'
                    and t2.publisher='ICIS'
                    and year(t1.startLocTimeStamp)='{yearstamp}' """


def get_carbon():
    """ Go through sets of function to retrieve CARBON historical data and forward data and merge them both, then finally make the conversion with the exchange rates
    :return:
    """
    # historical carbon
    carbon_data = get_historical_data(carbon_sql)
    carbon_data.index = [date_.strftime(format="%Y-%m-%d") for date_ in
                         pd.to_datetime(carbon_data['utcTimeStamp'])]

    # Build empty table with full yearly date values as index
    end_value = max(carbon_data.index)
    new_table = build_date_index(carbon_data, end_value)

    # Build up forward curve with data available and fillna
    carbon_data = append_rows(carbon_data, new_table)
    carbon_data['utcTimeStamp'] = carbon_data.index
    commodity, market, maxUtcTimeStamp = np.unique(carbon_data.commodity)[0], np.unique(carbon_data.market)[0], max(carbon_data.index).strftime("%Y-%m-%d")

    # Get forward data {Retrieve forward Data starting from max(date) of historical data}
    forward_data = get_forward_data(commodity, market, maxUtcTimeStamp)
    forward_data.index = forward_data.utcTimeStamp
    # Merge historic and forward data
    carbon_data = pd.concat([carbon_data, forward_data]).drop_duplicates('utcTimeStamp')

    # IF no rate conversion to be made, set appropriate columns for db table
    carbon_data = carbon_data.assign(**{'metric_change': 0,
                                        'price_usd_per_mwh': 0,
                                        'exchange_rate': 0,
                                        'price_euro_per_mwh': 0,
                                        'currency_change': 'nil',
                                        'metrics_name': 'nil',
                                        'modelrunDate': pd.datetime.now().date()}
                                     )
    carbon_data['price_euro_per_mwh'] = carbon_data.price

    # if rate conversion required, run below code
    # carbon_data= rate_conversion(carbon_data, full_exchange, metric_change=1.6282, metrics_name='Barrel_to_MW/h')
    return carbon_data

def get_brent(full_exchange):
    #Go through sets of function to retrieve BRENT historical data and forward data and merge them both, then finally make the conversion with the exchange rates
    # GET Brent data from table
    brent_data = get_historical_data(brent_sql)
    brent_data['utcTimeStamp'] = brent_data['contractName'].apply(set_date_time)
    brent_data.index = pd.to_datetime(brent_data['utcTimeStamp'], infer_datetime_format=True)

    # Build empty table with full yearly date values as index
    end_value = max(brent_data.index)
    new_table = build_date_index(brent_data, end_value)

    # Build up forward curve with data available and fillna
    brent_data = append_rows(brent_data, new_table)
    brent_data.utcTimeStamp =brent_data.index

    # No forward data for brent.. if required run the below code line
    # Retrieve forward Data starting from max(date) of historical data
    # forward_data = get_forward_data(commodity, market, maxUtcTimeStamp)

    # Finally convert to preferred currency and measure metric
    brent_data = rate_conversion(brent_data, full_exchange, metric_change=1.6282, metrics_name='Barrel_to_MW/h')
    brent_data['currency_change'] = 'USD_EUR'

    return brent_data

def get_gas():
        #Go through sets of function to retrieve GAS historical data and forward data and merge them both, then finally make the conversion with the exchange rates
        #gas
        gas_data = get_historical_data(gas_sql)
        gas_data.index = [date_.strftime(format="%Y-%m-%d") for date_ in
                          pd.to_datetime(gas_data['utcTimeStamp'])]  # , format="%Y-%m-%d",exact=False)]

        # Build empty table with full yearly date values as index
        end_value = max(gas_data.index)
        new_table = build_date_index(gas_data, end_value)
        # Build up forward curve with data available and fillna
        gas_data = append_rows(gas_data, new_table)
        gas_data['utcTimeStamp'] = gas_data.index

        # Get forward data {Retrieve forward Data starting from max(date) of historical data}
        commodity, market, maxUtcTimeStamp = np.unique(gas_data.commodity)[0], np.unique(gas_data.market)[0], max(
            gas_data.index).strftime("%Y-%m-%d")
        forward_data = get_forward_data(commodity, market, maxUtcTimeStamp)
        forward_data.index = forward_data.utcTimeStamp

        # Merge historic and forward data
        gas_data = pd.concat([gas_data, forward_data]).drop_duplicates('utcTimeStamp')
        gas_data['price_euro_per_mwh'] = gas_data.price
        return gas_data

def get_coal(full_exchange):
    #Go through sets of function to retrieve COAL historical data and forward data and merge them both, then finally make the conversion with the exchange rates
    # coal
    coal_data = get_historical_data(coal_sql)
    coal_data.index = [date_.strftime(format="%Y-%m-%d") for date_ in
                       pd.to_datetime(coal_data['utcTimeStamp'])]  # , format="%Y-%m-%d",exact=False)]

    # Build empty table with full yearly date values as index
    end_value = max(coal_data.index)
    new_table = build_date_index(coal_data, end_value
                                 )
    # Build up forward curve with data available and fillna
    coal_data = append_rows(coal_data, new_table)
    coal_data['utcTimeStamp'] = coal_data.index

    # Get forward data {Retrieve forward Data starting from max(date) of historical data}
    commodity, market, maxUtcTimeStamp = np.unique(coal_data.commodity)[0], np.unique(coal_data.market)[0], max(
        coal_data.index).strftime("%Y-%m-%d")
    forward_data = get_forward_data(commodity, market, maxUtcTimeStamp)
    forward_data.index = forward_data.utcTimeStamp

    # Merge historic and forward data
    coal_data = pd.concat([coal_data, forward_data]).drop_duplicates('utcTimeStamp')

    # Finally convert to preferred currency and measure metric
    coal_data = rate_conversion(coal_data, full_exchange, metric_change=8.141, metrics_name='tonne_to_MW/h')

    return coal_data


@DecorateErrorHandling
def runMainFunction():
    runTimeController = current_thread().getRunTimeController()
    logger = runTimeController.logger.getLogger()

    # Get exchange rates, and build complete dateIndex with full year range
    full_exchange = getexchange(exchange_rate_sql)

    #INSERT CARBON DATA TO TABLE
    insertValuetoSQL(get_carbon())

    # INSERT BRENT DATA TO TABLE
    insertValuetoSQL(get_brent(full_exchange))

    # INSERT GAS DATA TO TABLE
    insertValuetoSQL(get_gas())

    # INSERT COAL DATA TO TABLE
    insertValuetoSQL(get_coal(full_exchange))

    logger.info('Done')


@DecorateErrorHandling
def starttest():
 debugMode = True
 displayLogsOnConsole = True
 insertLogsIntoDatabase = False
 runTimeController1 = RunTimeController("commodities_price_curve", debugMode, insertLogsIntoDatabase,
                                        displayLogsOnConsole)
 t1 = CustomThread(runTimeController=runTimeController1, target=runMainFunction, args=())
 t1.start()


if __name__ == "__main__":
 starttest()


