import schwab

import dotenv
import os
import traceback

import time
import datetime
import pytz

import asyncio
import polars as pl
import sqlalchemy
import pandas as pd

class SchwabOptions:
    # For timing functions
    def stopwatch_decorator(theFunction):

        def wrapper(*args, **kwargs):
            startTime = time.perf_counter()
            result = theFunction(*args, **kwargs)
            endTime = time.perf_counter()
            print(f"Function {theFunction.__name__} took {(endTime - startTime)} seconds to run")
            return result
        
        return wrapper


    # Authenticate with Schwab
    def create_schwab_client(self):
        # Using environment variables for security
        dotenv.load_dotenv()

        client = schwab.auth.client_from_token_file(
                            api_key=os.getenv('CLIENT_ID'), 
                            app_secret=os.getenv('CLIENT_SECRET'), 
                            token_path=os.getenv('TOKEN_PATH'), 
                            asyncio=True)
        
        return client


    @stopwatch_decorator
    # Extract Options data from TDA's API
    async def extract(self, schwab_client, ticker: str) -> list:

        # This code is meant for 0DTE contracts. You can change the days=0 get whatever amount of data you'd like
        end_date = datetime.date.today() + datetime.timedelta(days=int(0))

        # The API enjoys crashing every so often, just sorta how it is. Wrap it in a Try/Catch.
        try:
            # Important point is that it doesn't give you a *good* way to get specific data, so you're better off
            # getting everything and then distilling it down to what you care about.
            # Pull options chain data from the API
            response = await schwab_client.get_option_chain(ticker, contract_type=schwab_client.Options.ContractType.ALL,
                                    strike_range=schwab_client.Options.StrikeRange.ALL, to_date=end_date)

            # Sleep it if you're rate-limited
            if response.status_code == 429:
                print("Being rate-limited")

                # Sleep for 5 seconds.
                time.sleep(5)
            else:
                # The future container for options contracts data
                contracts = []

                # The pure options data. It comes in JSON format
                rawOptionsData = response.json()

                # For both Calls & Puts
                for contract_type in ['callExpDateMap', 'putExpDateMap']:
                    # convert data to a Dictionary
                    contract = dict(rawOptionsData)[contract_type]
                    # for easy access to the keys - in this case, all of the expirations and strikes
                    expirations = contract.keys()
                    # run through all the contracts in the expiration
                    for expiry in list(expirations):
                        strikes = contract[expiry].keys()
                        # run through all the strikes in each contract
                        for strike in list(strikes):
                            entry = contract[expiry][strike][0]
                            # Create list of dictionaries with the flattened JSON
                            contracts.append(entry)


                # add this to lessen the risk of getting rate-limited
                time.sleep(0.25)

                return contracts

        # If there was an error
        except Exception:
            # Print it
            traceback.print_exc()
            # and sleep for 1 second
            time.sleep(1)


    @stopwatch_decorator
    # Uses Polars to transform the data into what we want/need
    async def transform(self, ticker: str, contracts: list) -> pl.DataFrame:
        # get the Epoch for this data
        epoch = int(time.time())

        # With Schwab, Index tickers like SPX have a leading $ in their symbols. If it's there, get rid of it for the work we'll be being soon.
        ticker = ticker.replace('$', '')

        # Load in the data into a Polars dataframe
        df = pl.DataFrame(data=contracts)

        # All of the columns I actually want. We'll use this below
        endColumns = ['putCall', 'symbol', 'description', 'bid', 'ask', 'last', 'mark', 'bidSize', 'askSize', 'bidAskSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume', 'quoteTimeInLong', 
            'netChange', 'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'openInterest', 'theoreticalOptionValue', 'theoreticalVolatility', 'strikePrice', 'expirationDate', 'daysToExpiration', 'expirationType', 
            'lastTradingDay', 'percentChange', 'intrinsicValue', 'extrinsicValue', 'optionRoot', 'high52Week', 'low52Week', 'inTheMoney']

        # Clean up the data.
        # Select the columns I want and clean...
        finalDF = df.select(endColumns).with_columns(
            # Ints
            pl.col('bidSize', 'askSize', 'lastSize', 'totalVolume', 'quoteTimeInLong', 'openInterest', 'lastTradingDay').cast(pl.Int64),
            # Floats
            pl.col('bid', 'ask', 'last', 'mark', 'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'netChange', 'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'theoreticalOptionValue', 'theoreticalVolatility', 
                'strikePrice', 'daysToExpiration', 'percentChange', 'intrinsicValue', 'extrinsicValue', 'high52Week', 'low52Week').cast(pl.Float64),
            # Boolean
            pl.col('inTheMoney').cast(pl.Boolean),
            # Date
            pl.col('expirationDate').str.to_datetime(),
            # Create three new columns. For the Ticker name,
            (pl.lit(ticker)).alias("ticker"),
            # the time in which it was pulled,
            (pl.lit(datetime.datetime.now(pytz.timezone('US/Eastern')))).alias("theDatetime"),
            # and a column to hold the primary key, which is the ticker, the options contract string, and the epoch
            #(ticker + pl.col("symbol").str.slice(-15) + "." + pl.col("quoteTimeInLong").cast(pl.String)) .alias("theKey")
            (ticker + pl.col("symbol").str.slice(-15) + "." + str(epoch)).alias("theKey")
            # below, you need to rename these columns because they're keyword in SQL. There's gotta be a better way to do this, but i don't know it yet hah
        ).rename({"description": "theDescription", "last": "lastPrice"})

        # # Configure it to print all the columns if you need to see the data
        # pl.Config.set_tbl_cols(finalDF.width)

        return finalDF


    @stopwatch_decorator
    async def load(self, dataFrames, dbSchema: str, dbTable: str) -> None:
        # Written to use MariaDB, but 
        dbEngine = sqlalchemy.create_engine(f"mariadb+mariadbconnector://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{int(os.getenv('DB_PORT'))}/{dbSchema}?charset=utf8mb4", 
                                    connect_args={"ssl": {"ssl_ca": os.getenv('DB_SSL') }})
        
        # There's gotta be a much better way than going to Polars and then back to Pandas but I just haven't had the time to dig for it yet.
        df = dataFrames.to_pandas()
        df.to_sql(dbTable, dbEngine, if_exists='append', chunksize=100, index=False, method='multi')

        print("sucess")


    @stopwatch_decorator
    async def etl(self, schwab_client, ticker: str, dbSchema: str, dbTable: str) -> None:
        # Grab the inital data
        extractedData = await self.extract(schwab_client, ticker)
        transformedData = await self.transform(ticker, extractedData)
        await self.load(transformedData, dbSchema, dbTable)


async def main():
    tickers = ["$SPX", "SPY", "QQQ"]
    
    schwab_object = SchwabOptions()
    schwab_client = schwab_object.create_schwab_client()

    tasks = [schwab_object.etl(schwab_client, ticker, os.getenv('DB_SCHEMA'), os.getenv('DB_TABLE')) for ticker in tickers]

    await asyncio.gather(*tasks)
    

if __name__ == "__main__":
    asyncio.run(main())