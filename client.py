import asyncio
import logging
import sys


# python3的普通字符串要传输的话需要先编码，这里使用bytes
# 分三段，但拼起来是一整句话，空格也衔接上了
MESSAGES = [b'This is the message, ',
            b'it will be sent ',
            b'in parts.', ]

# 要访问的地址
SERVER_ADDRESS = ('192.168.32.147', 8081)

# 相同的配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(name)s: %(message)s',
                    stream=sys.stderr)


class EchoClient(asyncio.Protocol):
    def __init__(self,future,messages):
        super().__init__()
        self.messages = messages  # 要传的信息
        self.f = future  # 指示状态用

    def connection_made(self, transport) -> None:
        """asyncio.create_connection()会调用这个方法以开始连接。同样要在这里完成客户端主要的事情"""
        self.transport = transport
        self.address = transport.get_extra_info('peername')
        self.log = logging.getLogger('EchoClient_{}_{}'.format(*self.address))
        self.log.debug('connecting to {} port {}'.format(*self.address))

        # 看上去是分步发送消息，实际上底层网络代码可能会把多个消息结合到一起一次传输
        for msg in self.messages:
            length = len(msg)
            #length.to_bytes(length=2, byteorder='big', signed=True)
            msg = bytes(str(length),'utf-8')+msg
            print(msg)
            transport.write(msg)
            self.log.debug(f'sending {msg!r}')
        # transport.writelines(self.messages)  # 如果不需分步显示，则可以这样改进

        # 作为结束,不然停不下来
        if transport.can_write_eof():
            transport.write_eof()

    def data_received(self, data) -> None:
        self.log.debug(f'received {data!r}')

    def eof_received(self):
        """把这个函数删掉以后还是会正常结束（帮助文档里连这个函数都没写）"""
        self.log.debug('received EOF')
        self.transport.close()

    def connection_lost(self, exc) -> None:
        self.log.debug('server closed connection')
        self.transport.close()
        super().connection_lost(exc)  # 同样将exc交给父类处理
        self.f.set_result(True)  # 最后设置future为完成


async def main():
    log = logging.getLogger('main')
    event_loop = asyncio.get_running_loop()
    future = event_loop.create_future()  # 完成状态的指示器
    log.debug('waiting for client to complete')
    # 启动客户端，lambda传参
    await event_loop.create_connection(
        lambda: EchoClient(future, MESSAGES), *SERVER_ADDRESS)
    # 等待future被设为完成
    await future


asyncio.run(main())
