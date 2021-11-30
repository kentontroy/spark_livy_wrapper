import json
import logging
import requests
import time
import warnings
from dataclasses import dataclass, field
from requests.auth import HTTPBasicAuth

warnings.filterwarnings("ignore")

@dataclass
class LivyCall:
  url: str ="https://kdavis-talend-demo-gateway.se-sandb.a465-9q4k.cloudera.site/kdavis-talend-demo/cdp-proxy-api/livy"
  username: str = "kdavis"
  password: str = "6Satan!@#"
  jobConf:  str = "./jobconf.json"
  data:     str = field(init=False, repr=True, default="")
  
  def __post_init__(self):
    with open(self.jobConf) as f:
      self.data = json.load(f)

def submitJob(c: LivyCall) -> {}:
  resp = requests.post(
    c.url + "/batches",
    data=json.dumps(c.data),
    headers={"Content-Type": "application/json"},
    auth=HTTPBasicAuth(c.username, c.password),
    verify=False
  )
  logging.info("Spark submit response: ")
  logging.info(resp.json())
  logging.info("Header: ")
  logging.info(resp.headers)
  return resp.headers

def trackJob(c: LivyCall, respHdr: {}) -> None:
  status = ""
  sessionUrl = c.url + respHdr["Location"].split("/statements", 1)[0]
  logging.info(sessionUrl)
  while status != "success":
    stmtUrl = c.url + respHdr["Location"]
    stmtResp = requests.get(
      stmtUrl,
      headers={"Content-Type": "application/json"},
      auth=HTTPBasicAuth(c.username, c.password),
    )
    status = stmtResp.json()["state"]
    logging.info("Job status : " + status)
    lines = requests.get(
      sessionUrl + "/log",
      headers={"Content-Type": "application/json"},
      auth=HTTPBasicAuth(c.username, c.password),
    ).json()["log"]

    for line in lines:
      logging.info(line)
      if status == "dead":
        raise ValueError("Job Failed: " + status)
      if "progress" in stmtResp.json():
        logging.info("Progress: " + str(stmtResp.json()["progress"]))
      time.sleep(10)

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
  ) 
  c = LivyCall()
  resp = submitJob(c)
  trackJob(c, resp)
