import asyncio
import json
import database
from pymysql.converters import escape_string
def creat_DB():
    config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'db_name': 'request_headers',  # 数据库名
    'charset': 'utf8mb4'
}
    db = database.DB(config)
    return db

db = creat_DB()

def add_json2database(json_object,db):
    """

    自动化将json 处理为sql语句 以及相应的数据

    :param json_object:   要传入的json文件
    :param db:  指定的数据库连接
    """
    sql_key = json_object.keys()

    add_data = tuple([json_object[key] for key in sql_key])

    columns = tuple(sql_key).__str__().replace("'", "`")
    values_place = ['%s' for i in range(len(sql_key))]
    values_place = tuple(values_place).__str__().replace("'", "")

    sql = 'insert into request ' + columns + ' values ' + values_place
    db.add_data(sql, add_data)


# 1 使用async/await 定义协程
async def handle_client(reader, writer):
    # 从客户端读取数据
    data = await reader.read(1024)
    message = data.decode()
    # 获取客户端地址
    addr = writer.get_extra_info('peername')
    print(f"Received {message!r} from {addr!r}")
    print(message,type(message))
    try:
        json_object = json.loads(message)
        print("this str is  json:")
        add_json2database(
            json_object,db
        )
        print(json_object)
    except:
        print("this str is not json!")
    # 将收到的数据发送回客户端
    writer.write(data)
    # 强制数据发送并清空缓冲区
    await writer.drain()
    print(f"Send: {message!r}")
    writer.close()


# 1 使用async/await 定义协程
async def main():
    # 使用asyncio.start_server()函数创建一个TCP服务器 server
    # 指定要监听的IP地址和端口号，并将handle_client()函数作为处理器
    server = await asyncio.start_server(handle_client, '192.168.32.1', 8888)
    # 获取服务器地址
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')


    # 使用async with语句来运行TCP服务器，以便在退出时自动关闭它。
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    # 2 使用 asyncio.run 创建事件循环
    # 3 在事件循环 asyncio.run 中执行协程
    asyncio.run(main())

