import logging
from sqlalchemy import create_engine
import azure.functions as func
from config import pgs
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    engine = create_engine(pgs)

    sql = '''
        WITH avg_temp_tbl AS (
        SELECT date_trunc('day', date) "date", avg(temp) "temp"
        FROM "dkelly-proj/cbus_temps"."temp_log"
        GROUP BY 1
        ORDER BY 1)
        SELECT
        date, temp,
        avg(temp)
        over(order by date rows between 9 preceding and current row)
        as moving_avg
        FROM avg_temp_tbl;
        '''

    df = pd.read_sql(sql, con = engine, parse_dates = 'date').sort_values('date').to_json(orient = "records")

    return df

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
    #     return func.HttpResponse(
    #          "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #          status_code=200
    #     )
