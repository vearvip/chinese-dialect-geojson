import os
import pandas as pd    # type: ignore
import sqlite3


def read(sqlStr: str): 
  # 获取当前脚本所在目录
  SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

  # 连接到 SQLite 数据库（如果数据库文件不存在，则会自动创建）
  db_path = os.path.join(SCRIPT_DIR, '../input/mcpdict.db') 

  # 连接数据库并执行查询
  conn = sqlite3.connect(db_path)
  df = pd.read_sql_query(sqlStr, conn)  # 直接读取为DataFrame 

  # 转换为字典列表
  result = df.to_dict(orient='records')  # 每行转为字典 
  return result