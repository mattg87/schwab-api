import schwab

import polars as pl

import dotenv
import os

import traceback

import time
import datetime
import pytz

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

    
    # Authenticate with TDA
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
    # Pull Options data from TDA's API and package it into a cleaned Dataframe
    async def get_options_data(self, schwab_client, ticker: str) -> pl.DataFrame:

        # This code is meant for 0DTE contracts. You can change the days=0 get whatever amount of data you'd like
        end_date = datetime.date.today() + datetime.timedelta(days=0)

        # The API enjoys crashing every so often, just sorta how it is. Wrap it in a Try/Catch.
        try:
            # get the Epoch for this data
            epoch = int(time.time())

            # Important point is that TDA doesn't give you a *good* way to get specific data, so you're better off
            # getting everything and then distilling it down to what you care about.
            # Pull options chain data from TDA's API
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

                # With Schwab, Index tickers like SPX have a leading $ in their symbols. If it's there, get rid of it for the work we'll be being soon.
                ticker = ticker.replace('$', '')

                # Load in the data into a Polars dataframe
                df = pl.DataFrame(data=contracts)

                # All of the columns I actually want. We'll use this below
                endColumns = ['putCall', 'symbol', 'description', 'bid', 'ask', 'last', 'mark', 'bidSize', 'askSize', 'bidAskSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'totalVolume', 'tradeTimeInLong', 'quoteTimeInLong', 
                    'netChange', 'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'openInterest', 'timeValue', 'theoreticalOptionValue', 'theoreticalVolatility', 'strikePrice', 'expirationDate', 'daysToExpiration', 'expirationType', 
                    'lastTradingDay', 'percentChange', 'intrinsicValue', 'extrinsicValue', 'optionRoot', 'high52Week', 'low52Week', 'inTheMoney']
                
                # Clean up the data.
                # Select the columns I want and clean...
                finalDF = df.select(endColumns).with_columns(
                    # Ints
                    pl.col('bidSize', 'askSize', 'lastSize', 'totalVolume', 'tradeTimeInLong', 'quoteTimeInLong', 'openInterest', 'lastTradingDay').cast(pl.Int64),
                    # Floats
                    pl.col('bid', 'ask', 'last', 'mark', 'highPrice', 'lowPrice', 'openPrice', 'closePrice', 'netChange', 'volatility', 'delta', 'gamma', 'theta', 'vega', 'rho', 'timeValue', 'theoreticalOptionValue', 'theoreticalVolatility', 
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
                    (ticker + pl.col("symbol").str.slice(-15) + "." + str(epoch)) .alias("theKey")
                )

                # # Configure it to print all the columns if you need to see the data
                # pl.Config.set_tbl_cols(finalDF.width)

                return finalDF

        # If there was an error
        except Exception:
            # Print it
            traceback.print_exc()
            # and sleep for 1 second
            time.sleep(1)


# A simple example that will get 0dte SPX options chain data
async def main():

    schwab_object = SchwabOptions()

    schwab_client = schwab_object.create_schwab_client()

    spxData = await schwab_object.get_options_data(schwab_client, "$SPX")

    print(spxData)


if __name__ == "__main__":

    import asyncio
    
    asyncio.run(main())