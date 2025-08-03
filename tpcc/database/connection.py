import readline
import socket

from tpcc.config import HOST, PORT


class Client:
    MAX_MEM_BUFFER_SIZE = 8192

    sockfd = None

    def __init__(self) -> None:
        readline.parse_and_bind(r"'\e[A': history-search-backward")
        readline.parse_and_bind(r"'\e[B': history-search-forward")

        # parser = argparse.ArgumentParser()
        # parser.add_argument('-s', type=str, help='unix socket path')
        # parser.add_argument('-p', type=int, default=self.PORT_DEFAULT, help='server port')
        # args = parser.parse_args()

        # if args.s is not None:
        #     self.sockfd = self.__init_unix_sock(args.s)
        # else:
        #     self.sockfd = self.__init_tcp_sock(self.HOST, args.p)

        self.sockfd = self.__init_tcp_sock(HOST, PORT)

        if self.sockfd is None:
            print("error occurs when initialize Client")
            exit(-1)
        pass

    def __is_exit_command(self, cmd):
        return cmd in ["exit", "exit;", "bye", "bye;"]

    def __init_unix_sock(self, unix_sock_path) -> socket:
        try:
            sockfd = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sockfd.connect(unix_sock_path)
            return sockfd
        except Exception as e:
            print(f"failed to create unix socket. {str(e)}")
            return None

    def __init_tcp_sock(self, server_host, server_port):
        try:
            host = socket.gethostbyname(server_host)
            sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockfd.settimeout(30)  # 30 second timeout for connection
            sockfd.connect((host, server_port))
            return sockfd
        except Exception as e:
            print(f"gethostbyname or create socket error. {str(e)}")
            return None

    ## 下面是public的函数

    def send_cmd(self, cmd):
        # send 单条sql命令
        if cmd:
            try:
                self.sockfd.settimeout(300)  # 300 second timeout for operations
                self.sockfd.sendall(cmd.encode())
                recv_buf = self.sockfd.recv(self.MAX_MEM_BUFFER_SIZE)
                if not recv_buf:
                    print("Connection has been closed")
                    raise ConnectionError("Database connection closed unexpectedly")
                else:
                    # print(recv_buf.decode(), end="")
                    return recv_buf.decode()
            except socket.timeout:
                print("Socket timeout occurred during database operation")
                raise
            except Exception as e:
                print(f"Connection was broken: {str(e)}")
                raise ConnectionError(f"Database connection error: {str(e)}")

    def start_shell_client(self):
        # 启动shell client, 在命令行中输入sql命令那种形式
        # while 循环反复获取input
        while True:
            command = input("Rucbase_py> ")

            if not command:
                continue

            readline.add_history(command)

            if self.__is_exit_command(command):
                print("The shell client will be closed.")
                break

            try:
                self.sockfd.sendall(command.encode())
                recv_buf = self.sockfd.recv(self.MAX_MEM_BUFFER_SIZE)
                if not recv_buf:
                    print("Connection has been closed")
                    break
                else:
                    print(recv_buf.decode(), end="")
            except Exception as e:
                print(f"Connection was broken: {str(e)}")
                break

        # shell端和函数端使用的同一个sockfd, 还是不要在这里close()掉吧
        # self.sockfd.close()

    def close(self):
        self.sockfd.close()


if __name__ == "__main__":
    client = Client()
    client.start_shell_client()
    client.close()

    # try:
    #     sys.exit(main())
    # except KeyboardInterrupt:
    #     print("Bye.")
    #     sys.exit(0)
