"""
Send sample query to prediction engine
"""

import predictionio
import time
start_time = time.time()
engine_client = predictionio.EngineClient(url="http://localhost:8000")
print engine_client.send_query({"items": [2295], "blackList": [636],  "num": 100})
print("--- %s seconds ---" % (time.time() - start_time))
