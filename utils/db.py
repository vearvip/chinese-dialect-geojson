import os
import pandas as pd    # type: ignore
import sqlite3
from  utils.make_db import make_db

# 获取当前脚本所在目录  
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 数据库路径
db_path = os.path.join(SCRIPT_DIR, '../input/mcpdict.db') 

def ensure_db():  
  """
  确保数据库存在
  """
  if not os.path.exists(db_path): 
    make_db()


def read(sqlStr: str):  
  """
  读取数据库
  """
  ensure_db()

  # 连接数据库并执行查询
  conn = sqlite3.connect(db_path)
  df = pd.read_sql_query(sqlStr, conn)  # 直接读取为DataFrame 

  # 转换为字典列表
  result = df.to_dict(orient='records')  # 每行转为字典 
  return result