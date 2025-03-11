import logging, json, asyncio

from sqlalchemy import text

from fastapi import Response

from utils import (
    HTTPException,
    iam,
    parse_event,
    fetchone_to_dict,
    with_session
)


async def main(event, context, session_factory):
    try:
        print('evento')

        tenants = await session_factory.list_tenants() 
        print(tenants)

        async with session.begin():
            
            sql = """
                SELECT * 
                FROM algorithm_settings;
            """
            result = await session.execute(text(sql), {})
            result = fetchone_to_dict(result)
            if not result:
                raise HTTPException(status_code=404, detail={"message":"deu ruim"})
            print("hfdksjghkadhgflsdfg")
            print(result)

        return Response(status_code=200)


    except HTTPException as e:
        logging.exception('HTTPException') #tmp
        raise e
    
    except Exception as e:
        logging.exception("Unhandled error")
        raise HTTPException(status_code=500, detail={"message": "Internal Server Error", "error_code": "internal_server_error"})
    
# lambda_handler = with_section(main)

if __name__ == "__main__":
    print("teste")
    func = with_session(main)
    # func({}, {})

    asyncio.run(func({}, {}))